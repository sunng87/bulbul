(ns bulbul.seg.writer
  (:require [bulbul.seg :as seg]
            [bulbul.codec :as bc]
            [clojure.java.io :as io]
            [clojure.data.avl :as cda]
            [bulbul.protocol :as p])
  (:import [java.io RandomAccessFile]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.nio.file Files FileSystems Path
            OpenOption StandardOpenOption]
           [java.nio.file.attribute FileAttribute]
           [bulbul.seg SegmentLog]))

(defn create-segment-file [id index config]
  (let [file (seg/segment-file config id)
        raf (.getChannel (RandomAccessFile. file "rw"))
        hb (ByteBuffer/allocate seg/header-total-size)]
    (.put hb seg/magic-number)
    (.put hb ^byte seg/version)
    (.putInt hb id)
    (.putLong hb index)
    (.putInt hb (:max-size config))
    (.putInt hb (:max-entry config))
    (.put hb seg/header-retain-padding)

    (.write ^FileChannel raf ^ByteBuffer (.flip hb))

    {:fd raf
     :meta {:version seg/version
            :max-size (:max-size config)
            :max-entry (:max-entry config)
            :file file}
     :start-index index
     :last-index (atom (dec index))
     :id id}))

(defn seg-full? [seg new-buffer-size]
  (or
   ;; max-entries, last-index is includsive so we add 1 here
   (>= (+ 1 (- @(:last-index seg) (:start-index seg)))
       (-> seg :meta :max-entry))
   (>= (+ new-buffer-size bc/buffer-meta-size (.position ^FileChannel (:fd seg)))
       (-> seg :meta :max-size))))

(defn append-new-seg! [store new-seg]
  (swap! (.-state store)
         (fn [state]
           (-> state
               ;; append to writer-segs
               (update :writer-segs conj new-seg)))))

(defn append-entry! [store entry-data]
  (let [codec (:codec (.-config store))
        entry-buffer (bc/encode codec entry-data)
        seg (last (:writer-segs @(.-state store)))
        seg (if (or (nil? seg)
                    (seg-full? seg (.. entry-buffer position)))
              (let [new-seg (create-segment-file (if seg (inc (:id seg)) 0)
                                                 (if seg (inc @(:last-index seg)) 0)
                                                 (.-config store))]
                ;; flash previous seg
                (when (some? seg) (.force ^FileChannel (:fd seg) true))
                (append-new-seg! store new-seg)
                new-seg)
              seg)]
    (bc/wrap-crc32-block! (:fd seg) entry-buffer)
    (swap! (:last-index seg) inc)))

(defn append-entries! [store entries]
  (doseq [e entries]
    (append-entry! store e))
  (let [seg (last (:writer-segs @(.-state store)))]
    (.force ^FileChannel (:fd seg) false)))

(defn truncate-to-index! [store index]
  (let [[retained-segs _ truncated-segs] (cda/split-key {:start-index index}
                                                        (:writer-segs @(.-state store)))
        current-seg (last retained-segs)]
    (seg/move-to-index! current-seg index)
    (when-not (empty? truncated-segs)
      (seg/close-and-remove-segs! truncated-segs))
    (swap! (.-state store) assoc
           :writer-segs (seg/into-sorted-segs retained-segs))))

(defn- open-file-channel [& path-seg]
  (let [path (.. (FileSystems/getDefault)
                 (getPath (first path-seg) (into-array String (rest path-seg))))]
    (Files/createDirectories (.getParent path) (into-array FileAttribute []))
    (FileChannel/open path (into-array OpenOption [StandardOpenOption/WRITE StandardOpenOption/CREATE]))))

(extend-protocol p/LogStoreWriter
  SegmentLog
  (open-writer! [this]
    (let [dir (:directory (.-config this))
          lock-file (open-file-channel dir ".lock")]
      (if-let [lock (.tryLock lock-file)]
        (let [segs (seg/load-seg-directory dir)]
          (swap! (.-state this) assoc
                 :writer-segs segs
                 :writer-lock lock-file))
        (do
          (.close lock-file)
          (throw (IllegalStateException. "Directory is locked by another process."))))))

  (write! [this entry]
    (append-entries! this [entry]))

  (write-all! [this entries]
    (append-entries! this entries))

  (truncate! [this index]
    (truncate-to-index! this index))

  (flush! [this]
    (doseq [fd (map :fd (:writer-segs @(.-state this)))]
      (.force ^FileChannel fd true)))

  (close-writer! [this]
    (seg/close-seg-files! (:writer-segs @(.-state this)))
    (.close (:writer-lock @(.-state this)))
    (swap! (.-state this) dissoc :writer-segs :writer-lock)))
