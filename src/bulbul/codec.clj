(ns bulbul.codec
  (:import [java.nio ByteBuffer]))

;; TODO: fix ByteBuffer based api

;; macro to improve codec readability
(defmacro encoder [args & body]
  `(fn ~args ~@body))

(defmacro decoder [args & body]
  `(fn ~args ~@body))

(defmacro primitive-codec [sname size writer-fn reader-fn]
  `(defcodec ~sname
     (encoder [_# data# ^ByteBuffer buffer#]
              (. buffer# ~writer-fn data#)
              buffer#)
     (decoder [_# ^ByteBuffer buffer#]
              (when (>= (- (.limit buffer#) (.position buffer#)) ~size)
                (. buffer# ~reader-fn)))))

(primitive-codec byte 1 put get)
(primitive-codec int16 2 putShort getShort)
(primitive-codec int32 4 putInt getInt)
(primitive-codec int64 8 putLong getLong)
(primitive-codec float 4 putFloat getFloat)
(primitive-codec double 8 putDouble getDouble)

(defn- find-delimiter [^ByteBuf src ^bytes delim]
  (loop [sindex (.readerIndex src) dindex 0]
    (if (= sindex (.writerIndex src))
      -1
      (if (= ^Byte (.getByte src sindex) ^Byte (aget delim dindex))
        (if (= dindex (- (alength delim) 1))
          (+ (- sindex (.readerIndex src)) 1)
          (recur (inc sindex) (inc dindex)))
        (recur (inc sindex) 0)))))

(defcodec string
  (encoder [options ^String data ^ByteBuf buffer]
           (let [{:keys [prefix encoding delimiter]} options
                 encoding (name encoding)
                 bytes (.getBytes (or data "") encoding)]
             (cond
               (and (nil? delimiter) (nil? prefix))
               (throw (IllegalArgumentException. "Neither :delimiter nor :prefix provided for string codec"))
               ;; length prefix string
               (nil? delimiter)
               (do
                 ((:encoder prefix) (alength bytes) buffer)
                 (.writeBytes buffer ^bytes bytes))
               ;; delimiter based string
               (nil? prefix)
               (do
                 (.writeBytes buffer ^bytes bytes)
                 (.writeBytes buffer ^bytes
                              (.getBytes ^String delimiter encoding)))))
           buffer)
  (decoder [options ^ByteBuf buffer]
           (let [{:keys [prefix encoding delimiter]} options
                 encoding (name encoding)]
             (cond
               (and (nil? delimiter) (nil? prefix))
               (throw (IllegalArgumentException. "Neither :delimiter nor :prefix provided for string codec"))
               ;; length prefix string
               (nil? delimiter)
               (do
                 (when-let [byte-length ((:decoder prefix) buffer)]
                   (when-not (> byte-length (.readableBytes buffer))
                     (let [bytes (byte-array byte-length)]
                       (.readBytes buffer ^bytes bytes)
                       (String. bytes encoding)))))

               ;; delimiter based string
               (nil? prefix)
               (let [dbytes (.getBytes ^String delimiter encoding)
                     dmlength (alength ^bytes dbytes)
                     dlength (find-delimiter buffer dbytes)
                     slength (- dlength dmlength)]
                 (when (> slength 0)
                   (let [sbytes (byte-array slength)]
                     (.readBytes buffer ^bytes sbytes)
                     ;; move readerIndex
                     (.readerIndex buffer (+ dmlength (.readerIndex buffer)))
                     (String. sbytes encoding))))))))

(defcodec byte-block
  (encoder [options ^ByteBuf data ^ByteBuf buffer]
           (let [{prefix :prefix encode-length-fn :encode-length-fn} options
                 encode-length-fn (or encode-length-fn identity)
                 byte-length (if (nil? data) 0 (.readableBytes data))
                 encoded-length (encode-length-fn byte-length)]
             ((:encoder prefix) encoded-length buffer)
             (when-not (nil? data)
               (.writeBytes buffer ^ByteBuf data)
               (.release ^ByteBuf data))
             buffer))
  (decoder [options ^ByteBuf buffer]
           (let [{prefix :prefix decode-length-fn :decode-length-fn} options
                 decode-length-fn (or decode-length-fn identity)
                 byte-length (decode-length-fn ((:decoder prefix) buffer))]
             (when-not (or (nil? byte-length)
                           (> byte-length (.readableBytes buffer)))
               (.readRetainedSlice buffer byte-length)))))

(def ^{:private true} reversed-map
  (memoize
   (fn [m]
     (apply hash-map (mapcat #(vector (val %) (key %)) m)))))

(defcodec enum
  (encoder [options data ^ByteBuf buffer]
           (let [[codec mapping] options
                 mapping (if (util/derefable? mapping) @mapping mapping)
                 value (get mapping data)]
             ((:encoder codec) value buffer)))
  (decoder [options ^ByteBuf buffer]
           (let [[codec mapping] options
                 mapping (if (util/derefable? mapping) @mapping mapping)
                 mapping (reversed-map mapping)
                 value ((:decoder codec) buffer)]
             (get mapping value))))

(defcodec header
  (encoder [options data ^ByteBuf buffer]
           (let [[enumer children] options
                 head (first data)
                 body (second data)
                 body-codec (get children head)]
             ((:encoder enumer) head buffer)
             ((:encoder body-codec) body buffer)
             buffer))
  (decoder [options ^ByteBuf buffer]
           (let [[enumer children] options
                 head ((:decoder enumer) buffer)
                 body (and head ;; body is nil if head is nil
                           ((:decoder (get children head)) buffer))]
             (if-not (nil? body)
               [head body]
               (do
                 (try-release body)
                 nil)))))

(defcodec frame
  (encoder [options data ^ByteBuf buffer]
           (let [codecs options]
             (dorun (map #((:encoder %1) %2 buffer) codecs data))
             buffer))
  (decoder [options ^ByteBuf buffer]
           (let [codecs options]
             (loop [c codecs r []]
               (if (empty? c)
                 r
                 (if-let [r0 ((:decoder (first c)) buffer)]
                   (recur (rest c) (conj r r0))
                   (do
                     (try-release r)
                     nil)))))))

(defcodec counted
  (encoder [options data ^ByteBuf buffer]
           (let [length (count data)
                 {length-codec :prefix body-codec :body} options]
             ((:encoder length-codec) length buffer)
             (doseq [frm data]
               ((:encoder body-codec) frm buffer))
             buffer))
  (decoder [options ^ByteBuf buffer]
           (let [{length-codec :prefix body-codec :body} options
                 length ((:decoder length-codec) buffer)]
             (when length
               (loop [idx 0 results []]
                 (if (== idx length)
                   results
                   (if-let [data ((:decoder body-codec) buffer)]
                     (recur (inc idx) (conj results data))
                     (do
                       (try-release results)
                       nil))))))))

(defcodec const
  (encoder [options data ^ByteBuf buffer]
           buffer)
  (decoder [options ^ByteBuf buffer]
           (first options)))

(defn encode*
  ([codec data ^ByteBuf buffer]
   ((:encoder codec) data buffer)))

(defn decode* [codec ^ByteBuf buffer]
  ((:decoder codec) buffer))