(ns bulbul.seg
  (:require [bulbul.protocol :as p]
            [bulbul.codec :as bc]
            [clojure.java.io :as io]
            [clojure.data.avl :as cda])
  (:import [java.io RandomAccessFile]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]))

(def magic-number (.getBytes "BULO" "UTF-8"))
(def version (byte 1))
(def header-total-size 128)
(def header-current-size
  (+ (alength magic-number)
     1 ;; version
     4 ;; id
     8 ;; index
     4 ;; max-size
     4 ;; max-entry
     ))
(def header-retain-size (- header-total-size header-current-size))
(assert (>= header-retain-size 0))
(def header-retain-padding (byte-array header-retain-size))

(defn segment-file [config id]
  (io/file (str (:directory config) "/" (:name config) ".log." id)))

(defrecord SegmentLog [state config])

(defn segment-log-initial-state []
  {})

(defn segment-log-default-config []
  {:directory "./bulbullog"
   :name "bulbul"
   :max-entry 1000000
   :max-size (* 20 1024 1024)
   :version version})

(defn segment-log [codec config]
  (let [state (atom (segment-log-initial-state))
        config (merge (segment-log-default-config) config {:codec codec})]
    (SegmentLog. state config)))

(defn move-to-index! [seg search-index]
  (let [file-channel ^FileChannel (:fd seg)
        current-index (if (<= search-index @(:last-index seg))
                        (do
                          (.position file-channel ^long header-total-size)
                          (:start-index seg))
                        ;; move to a search-index that larger than
                        ;; current last index
                        (inc @(:last-index seg)))
        file-size (.size file-channel)
        [integrity index] (loop [idx current-index]
                            (if (< (.position file-channel) file-size)
                              (if (bc/unwrap-crc32-block file-channel)
                                (let [next-idx (inc idx)]
                                  (if (= next-idx search-index)
                                    ;; return the search index, with
                                    ;; the item unread
                                    [true idx]
                                    (recur next-idx)))
                                ;; dec to last valid id
                                [false (dec idx)])
                              ;; dec to last valid id
                              [true (dec idx)]))]
    ;; keep the index sync with file cursor
    (reset! (:last-index seg) index)
    [integrity index]))

(defn open-segment-file [file]
  (let [raf (.getChannel (RandomAccessFile. file "rw"))
        hb (ByteBuffer/allocate header-total-size)
        header-size (.read raf hb)]
    (if (= header-size header-total-size)
      (let [magic-number-array (byte-array (alength magic-number))]
        (.flip hb)
        (.get hb magic-number-array)
        (if (= (seq magic-number) (seq magic-number-array))
          (let [version (.get hb)
                id (.getInt hb)
                index (.getLong hb)
                max-size (.getInt hb)
                max-entry (.getInt hb)]
            {:fd raf
             :meta {:version version
                    :max-size max-size
                    :max-entry max-entry
                    :file file}
             :start-index index
             :last-index (atom (dec index))
             :id id})
          ;; not a bulbul file
          (.close raf)))
      (.close raf))))

(defn close-and-remove-segs! [index-file-map]
  (doseq [{fd :fd {file :file} :meta} index-file-map]
    (.close fd)
    (.delete file)))

(defn empty-tree []
  (cda/sorted-set-by #(< (:start-index %1) (:start-index %2))))

(defn verify-segment-files [index-file-maps]
  (loop [segs index-file-maps result (empty-tree) previous-last-index -1]
    (if-let [current-seg (first segs)]
      (if (= previous-last-index (dec (:start-index current-seg)))
        (let [[integrity last-index] (move-to-index! current-seg -1)]
          (if integrity
            (recur (rest segs)
                   (conj result current-seg)
                   last-index)
            (do
              (close-and-remove-segs! segs)
              result)))
        (do
          (close-and-remove-segs! segs)
          result))
      result)))

(defn into-sorted-segs [segs]
  (into (empty-tree) segs))

(defn load-seg-files [files]
  (->> files
       (mapv open-segment-file)
       (filter some?)
       into-sorted-segs
       verify-segment-files))

(defn load-seg-directory [dir]
  (let [dir (doto (io/file dir)
              (.mkdirs))]
    (load-seg-files (into [] (.listFiles dir)))))

(defn close-seg-files! [segs]
  (doseq [s segs]
    (.close (:fd s))))

#_(defn reset-to-index! [segs index]
  (let [the-seg (first (drop-while #(>= (:start-index %) index)))]
    (move-to-index! the-seg index)))
