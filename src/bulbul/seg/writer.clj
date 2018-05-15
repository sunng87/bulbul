(ns bulbul.seg.writer
  (:require [bulbul.seg :as seg]
            [bulbul.codec :as bc]
            [clojure.java.io :as io]
            [clojure.data.avl :as cda]
            [bulbul.protocol :as p])
  (:import [java.io RandomAccessFile]
           [java.nio ByteBuffer]
           [bulbul.seg SegmentLog]))

(defn segment-file [config id]
  (io/file (str (:directory config) "/" (:name config) ".log." id)))

(defn create-segment-file [id index config]
  (let [file (segment-file config id)
        raf (.getChannel (RandomAccessFile. file "rw"))
        hb (ByteBuffer/allocate seg/header-total-size)]
    (.put hb seg/magic-number)
    (.putInt hb seg/version)
    (.putInt raf id)
    (.putLong raf index)
    (.putInt (:max-size config))
    (.putInt (:max-entry config))

    (.write raf hb)

    {:fd raf
     :meta {:version seg/version
            :max-size (:max-size config)
            :max-entry (:max-entry config)
            :file file}
     :start-index index
     :last-index (atom index)
     :id id}))

(defn seg-full? [seg new-buffer-size]
  (or
   ;; max-entries
   (> (- @(:last-index seg) (:start-index seg))
      (-> seg :meta :max-entry))
   (> (+ new-buffer-size bc/buffer-meta-size (.position (:fd seg)))
      (-> seg :meta :max-size))))

(defn append-entry! [store entry-data]
  (let [codec (:codec (.-config store))
        entry-buffer (bc/encode codec entry-data)
        seg (last (:writer-segs @(.-state store)))
        seg (if (seg-full? seg (.. entry-buffer flip remaining))
              (let [new-seg (create-segment-file (inc (:id seg))
                                                 (inc @(:last-index seg))
                                                 (.-config store))]
                (swap! (.-state store) update :writer-segs conj new-seg)
                new-seg)
              seg)]
    (bc/wrap-crc32-block! (:fd seg) entry-buffer)
    (swap! (:last-index seg) inc)))

(defn truncate-to-index! [store index]
  (let [[truncated-segs _ retained-segs] (cda/split-key {:start-index index}
                                                        (:writer-segs @(.-state store)))
        current-seg (first retained-segs)]
    (seg/move-to-index! current-seg index)
    (seg/close-and-remove-segs! truncated-segs)
    (swap! (.-state store) assoc :writer-segs (seg/into-sorted-segs retained-segs))))

(extend-protocol p/LogStoreWriter
  SegmentLog
  (open-writer! [this]
    (let [logs (seg/load-seg-directory (:directory (.-config this)))]
      (swap! (.-state this) assoc
             :writer-segs logs)))

  (write! [this entry]
    (append-entry! this entry))

  (truncate! [this index]
    (truncate-to-index! this index))

  (flush! [this]
    ;; TODO:
    )

  (close-writer! [this]
    (seg/close-seg-files! (:writer-segs @(.-state this)))))
