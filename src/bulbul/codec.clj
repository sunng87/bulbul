(ns bulbul.codec
  (:import [java.nio Buffer ByteBuffer]))

;; TODO: fix ByteBuffer based api
(defmacro defcodec [sym encoder-fn decoder-fn]
  `(defn ~sym [& options#]
     {:encoder (partial ~encoder-fn options#)
      :decoder (partial ~decoder-fn options#)}))


;; macro to improve codec readability
(defmacro encoder [args & body]
  `(fn ~args ~@body))

(defmacro decoder [args & body]
  `(fn ~args ~@body))

(defn- ensure-buffer [buffer size]
  (let [space (.remaining buffer)]
    (if (< space size)
      (doto (ByteBuffer/allocate (* 2 (.limit buffer)))
        (.put (.flip buffer)))
      buffer)))

(defmacro primitive-codec [sname size writer-fn reader-fn]
  `(defcodec ~sname
     (encoder [_# data# ^ByteBuffer buffer#]
              (doto (ensure-buffer buffer# ~size)
                (. ~writer-fn data#)))
     (decoder [_# ^ByteBuffer buffer#]
              (when (>= (.remaining buffer#) ~size)
                (. buffer# ~reader-fn)))))

(primitive-codec byte 1 put get)
(primitive-codec int16 2 putShort getShort)
(primitive-codec int32 4 putInt getInt)
(primitive-codec int64 8 putLong getLong)
(primitive-codec float 4 putFloat getFloat)
(primitive-codec double 8 putDouble getDouble)

(defn- find-delimiter [^ByteBuffer src ^bytes delim]
  (loop [sindex (.position src) dindex 0]
    (if (= sindex (.limit src))
      -1
      (if (= ^Byte (.get src sindex) ^Byte (aget delim dindex))
        (if (= dindex (- (alength delim) 1))
          (+ (- sindex (.position src)) 1)
          (recur (inc sindex) (inc dindex)))
        (recur (inc sindex) 0)))))

(defcodec string
  (encoder [options ^String data ^ByteBuffer buffer]
           (let [{:keys [prefix encoding delimiter]} options
                 encoding (name encoding)
                 bytes (.getBytes ^String (or data "") ^String encoding)]
             (cond
               (and (nil? delimiter) (nil? prefix))
               (throw (IllegalArgumentException. "Neither :delimiter nor :prefix provided for string codec"))
               ;; length prefix string
               (nil? delimiter)
               (let [length (alength bytes)]
                 (as-> buffer $buf
                   ((:encoder prefix) length $buf)
                   (.put (ensure-buffer $buf length) ^bytes bytes)))
               ;; delimiter based string
               (nil? prefix)
               (let [delimiter-bytes (.getBytes ^String delimiter encoding)]
                 (as-> buffer $buf
                   (.put (ensure-buffer $buf (alength bytes)) ^bytes bytes)
                   (.put (ensure-buffer $buf (alength delimiter-bytes))
                         ^bytes delimiter-bytes)))))
           buffer)
  (decoder [options ^ByteBuffer buffer]
           (let [{:keys [prefix encoding delimiter]} options
                 encoding (name encoding)]
             (cond
               (and (nil? delimiter) (nil? prefix))
               (throw (IllegalArgumentException. "Neither :delimiter nor :prefix provided for string codec"))
               ;; length prefix string
               (nil? delimiter)
               (do
                 (when-let [byte-length ((:decoder prefix) buffer)]
                   (when-not (> byte-length (.remaining buffer))
                     (let [bytes (byte-array byte-length)]
                       (.get buffer ^bytes bytes)
                       (String. bytes encoding)))))

               ;; delimiter based string
               (nil? prefix)
               (let [dbytes (.getBytes ^String delimiter encoding)
                     dmlength (alength ^bytes dbytes)
                     dlength (find-delimiter buffer dbytes)
                     slength (- dlength dmlength)]
                 (when (> slength 0)
                   (let [sbytes (byte-array slength)]
                     (.get buffer ^bytes sbytes)
                     ;; move readerIndex
                     (.position buffer (+ dmlength (.position buffer)))
                     (String. sbytes encoding))))))))

(defcodec byte-block
  (encoder [options ^bytes data ^ByteBuffer buffer]
           (let [{prefix :prefix encode-length-fn :encode-length-fn} options
                 encode-length-fn (or encode-length-fn identity)
                 byte-length (if (nil? data) 0 (alength data))
                 encoded-length (encode-length-fn byte-length)]
             (as-> buffer $buf
               ((:encoder prefix) encoded-length buffer)
               (if (some? data)
                 (.put (ensure-buffer $buf byte-length) data)
                 $buf))))
  (decoder [options ^ByteBuffer buffer]
           (let [{prefix :prefix decode-length-fn :decode-length-fn} options
                 decode-length-fn (or decode-length-fn identity)
                 byte-length (decode-length-fn ((:decoder prefix) buffer))]
             (when-not (or (nil? byte-length)
                           (> byte-length (.remaining buffer)))
               (let [result-bytes (byte-array byte-length)]
                 (.get buffer result-bytes))))))

(def ^{:private true} reversed-map
  (memoize
   (fn [m]
     (apply hash-map (mapcat #(vector (val %) (key %)) m)))))

(defcodec enum
  (encoder [options data ^ByteBuffer buffer]
           (let [[codec mapping] options
                 value (get mapping data)]
             ((:encoder codec) value buffer)))
  (decoder [options ^ByteBuffer buffer]
           (let [[codec mapping] options
                 mapping (reversed-map mapping)
                 value ((:decoder codec) buffer)]
             (get mapping value))))

(defcodec header
  (encoder [options data ^ByteBuffer buffer]
           (let [[enumer children] options
                 head (first data)
                 body (second data)
                 body-codec (get children head)]
             (doto buffer
               ((:encoder enumer) head buffer)
               ((:encoder body-codec) body buffer))))
  (decoder [options ^ByteBuffer buffer]
           (let [[enumer children] options
                 head ((:decoder enumer) buffer)
                 body (and head ;; body is nil if head is nil
                           ((:decoder (get children head)) buffer))]
             (when-not (nil? body)
               [head body]))))

(defcodec record
  (encoder [options data ^ByteBuffer buffer]
           (let [codecs options]
             (loop [$buf buffer $codecs codecs $data data]
               (if (not-empty $codecs)
                 (recur ((:encoder (first $codecs)) (first $data) $buf)
                        (rest $codecs) (rest $data))
                 $buf))))
  (decoder [options ^ByteBuffer buffer]
           (let [codecs options]
             (loop [c codecs r []]
               (if (empty? c)
                 r
                 (when-let [r0 ((:decoder (first c)) buffer)]
                   (recur (rest c) (conj r r0))))))))

(defcodec counted
  (encoder [options data ^ByteBuffer buffer]
           (let [length (count data)
                 {length-codec :prefix body-codec :body} options]
             (loop [$buf ((:encoder length-codec) length buffer) $data data]
               (if (not-empty $data)
                 (recur ((:encoder body-codec) (first $data) $buf) (rest $data))
                 $buf))))
  (decoder [options ^ByteBuffer buffer]
           (let [{length-codec :prefix body-codec :body} options
                 length ((:decoder length-codec) buffer)]
             (when length
               (loop [idx 0 results []]
                 (if (== idx length)
                   results
                   (when-let [data ((:decoder body-codec) buffer)]
                     (recur (inc idx) (conj results data)))))))))

(defcodec const
  (encoder [options data ^ByteBuffer buffer]
           buffer)
  (decoder [options ^ByteBuffer buffer]
           (first options)))

(defn encode
  ([codec data ^ByteBuffer buffer]
   ((:encoder codec) data buffer))
  ([codec data]
   (encode codec data (ByteBuffer/allocate 256))))

(defn decode [codec ^ByteBuffer buffer]
  ((:decoder codec) buffer))
