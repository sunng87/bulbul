(ns bulbul.seg
  (:require [bulbul.protocol :as p]
            [bulbul.codec :as bc]
            [clojure.java.io :as io])
  (:import [java.io RandomAccessFile]
           [java.nio ByteBuffer]))

(def magic-number (.getBytes "BULBULLOG1" "UTF-8"))
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
  {:index 0
   :id 0})

(defn segment-log-default-config []
  {:directory "./bulbul_log"
   :max-entry 1000000
   :max-size (* 5 1024 1024)
   :version version})

(defn segment-log [config]
  (let [state (atom (segment-log-initial-state))
        config (merge (segment-log-default-config) config)]
    (SegmentLog. state config)))

(defn- load-last-index [fd]
  (let [current-index (:last-index fd)
        file-channel (:fd fd)
        file-size (.size file-channel)]
    (loop [idx current-index]
      (if (< (.position file-channel) (dec file-size))
        (if (bc/unwrap-crc32-block file-channel)
          (recur (inc idx))
          (do
            [false idx]))
        [true idx]))))

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
             :id id})
          ;; not a bulbul file
          (.close raf)))
      (.close raf))))

(defn- remove-invalid-files [rest-index-file-map]
  (doseq [{fd :fd {file :file} :meta} rest-index-file-map]
    (.close fd)
    (.delete file)))

(defn- load-segment-files [index-file-map]
  (loop [segs index-file-map result [] previous-last-index -1]
    (if-let [current-seg (first segs)]
      (if (= previous-last-index (dec (:start-index current-seg)))
        (let [[integrity last-index] (load-last-index (:fd current-seg))]
          (if integrity
            (recur (rest index-file-map)
                   (conj result (assoc current-seg :last-index (atom last-index)))
                   last-index)
            (do
              (remove-invalid-files (rest index-file-map))
              result)))
        (do
          (remove-invalid-files (rest index-file-map))
          result))
      result)))

(defn- segment-file [config id]
  (io/file (str (:directory config) "/" (:name config) ".log." id)))

(defn create-segment-file [id index config]
  (let [file (segment-file config id)
        raf (.getChannel (RandomAccessFile. file "rw"))
        hb (ByteBuffer/allocate header-total-size)]
    (.put hb magic-number)
    (.putInt hb (:version config))
    (.putInt raf id)
    (.putLong raf index)
    (.putInt (:max-size config))
    (.putInt (:max-entry config))

    (.write raf hb)

    {:fd raf
     :meta {:version version
            :max-size (:max-size config)
            :max-entry (:max-entry config)
            :file file}
     :start-index index
     :last-index (atom index)
     :id id}))

(defn load-seg-directory [dir]
  (let [dir (doto (io/file dir)
              (.mkdirs))]
    (->> (file-seq dir)
         (map open-segment-file)
         doall
         (filter some?)
         (sort-by (comp - :index))
         load-segment-files)))

(defn close-seg-files [files]
  (-> files
      (map #(.close (:fd %)))
      (dorun)))

(defn seg-full? [seg new-buffer-size]
  (or
   ;; max-entries
   (> (- @(:last-index seg) (:start-index seg))
      (-> seg :meta :max-entry))
   (> (+ new-buffer-size bc/buffer-meta-size (.position (:fd seg)))
      (-> seg :meta :max-size))))

(defn append-entry [store entry-data]
  (let [codec (:codec (.-config store))
        entry-buffer (bc/encode codec entry-data)
        seg (first (:segs @(.-state store)))
        seg (if (seg-full? seg (.. entry-buffer flip remaining))
              (let [new-seg (create-segment-file (inc (:id seg))
                                                 (inc @(:last-index seg))
                                                 (.-config store))]
                ;; FIXME: order
                (swap! (.-state store) update :segs conj new-seg)
                new-seg)
              seg)]
    (bc/wrap-crc32-block! (:fd seg) entry-buffer)
    (swap! (:last-index seg) inc)))

(extend-protocol p/LogStore
  SegmentLog
  (open! [this]
    (let [logs (load-seg-directory (:directory (.-config this)))]
      (swap! (.-state this) assoc
             :segs logs
             :open? true)))

  (write! [this entry]
    (append-entry this entry))

  (reset-index! [this index])

  (flush! [this])

  (read [this])

  (close! [this]
    (close-seg-files (:files @(.-state this)))
    (swap! (.-state this) assoc :open? false)))
