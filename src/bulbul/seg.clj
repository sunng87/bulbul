(ns bulbul.seg
  (:require [bulbul.protocol :as p]
            [bulbul.codec :as bc]
            [clojure.java.io :as io]
            [clojure.data.avl :as cda])
  (:import [java.io RandomAccessFile]
           [java.nio ByteBuffer]))

(def magic-number (.getBytes "BULO" "UTF-8"))
(def version 1)
(def header-total-size 128)
(def header-current-size
  (+ (alength magic-number)
     2 ;; version
     2 ;; id
     4 ;; index
     2 ;; max-size
     2 ;; max-entry
     ))
(def header-retain-size (- header-total-size header-current-size))

(defrecord SegmentLog [state config])

(defn segment-log-initial-state []
  {})

(defn segment-log-default-config []
  {:directory "./bulbul_log"
   :max-entry 1000000
   :max-size (* 200 1024 1024)
   :version version})

(defn segment-log [codec config]
  (let [state (atom (segment-log-initial-state))
        config (merge (segment-log-default-config) config {:codec codec})]
    (SegmentLog. state config)))

(defn move-to-index! [seg search-index]
  (let [file-channel (:fd seg)
        current-index (if (< search-index @(:last-index seg))
                        (do
                          (.position file-channel header-total-size)
                          (:start-index seg))
                        @(:last-index seg))
        file-size (.size file-channel)
        [integrity index] (loop [idx current-index]
                            (if (< (.position file-channel) (dec file-size))
                              (if (bc/unwrap-crc32-block file-channel)
                                (let [next-idx (inc idx)]
                                  (if (= next-idx search-index)
                                    [true idx]
                                    (recur next-idx)))
                                [false idx])
                              [true idx]))]
    ;; keep the index sync with file cursor
    (reset! (:last-index seg) index)
    [integrity index]))

(defn open-segment-file [file]
  (let [raf (.getChannel (RandomAccessFile. file "rw"))
        hb (ByteBuffer/allocate header-total-size)
        header-size (.read raf hb)]
    (if (= header-size header-total-size)
      (let [magic-number-array (byte-array (alength magic-number))]
        (.get hb magic-number-array)
        (if (= (seq magic-number) (seq magic-number-array))
          (let [version (.getInt hb)
                id (.getInt hb)
                index (.getLong raf)
                max-size (.getInt raf)
                max-entry (.getInt raf)]
            {:fd raf
             :meta {:version version
                    :max-size max-size
                    :max-entry max-entry
                    :file file}
             :start-index index
             :last-index (atom index)
             :id id})
          ;; not a bulbul file
          (.close raf)))
      (.close raf))))

(defn close-and-remove-segs! [index-file-map]
  (doseq [{fd :fd {file :file} :meta} index-file-map]
    (.close fd)
    (.delete file)))

(defn load-segment-files [index-file-map]
  (loop [segs index-file-map result [] previous-last-index -1]
    (if-let [current-seg (first segs)]
      (if (= previous-last-index (dec (:start-index current-seg)))
        (let [[integrity last-index] (move-to-index! (:fd current-seg) -1)]
          (if integrity
            (recur (rest index-file-map)
                   (conj result current-seg)
                   last-index)
            (do
              (close-and-remove-segs! (rest index-file-map))
              result)))
        (do
          (close-and-remove-segs! (rest index-file-map))
          result))
      result)))

(defn into-sorted-segs [segs]
  (into (cda/sorted-set-by #(< (:start-index %1) (:start-index %2))) segs))

(defn load-seg-directory [dir]
  (let [dir (doto (io/file dir)
              (.mkdirs))]
    (->> (file-seq dir)
         (map open-segment-file)
         doall
         (filter some?)
         into-sorted-segs
         load-segment-files)))

(defn close-seg-files! [files]
  (-> files
      (map #(.close (:fd %)))
      (dorun)))

#_(defn reset-to-index! [segs index]
  (let [the-seg (first (drop-while #(>= (:start-index %) index)))]
    (move-to-index! the-seg index)))
