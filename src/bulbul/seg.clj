(ns bulbul.seg
  (:require [bulbul.protocol :as p]
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

(defn load-last-index [file-fd]
  (let [current-index -1]
    ;; TODO: loop over the fd to get latest index
    ))

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
                    :max-entry max-entry}
             :start-index index
             :id id})
          ;; not a bulbul file
          (.close raf)))
      (.close raf))))

(defn- segment-file-name [config id]
  (str (:directory config) "/" (:name config) ".log." id))

(defn create-segment-file [id index config]
  (let [raf (.getChannel (RandomAccessFile. (segment-file-name config id) "rw"))
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
            :max-entry (:max-entry config)}
     :start-index index
     :last-index index
     :id id}))

(defn load-seg-directory [dir]
  (let [dir (io/file dir)]
    (->> (file-seq dir)
         (map open-segment-file)
         doall
         (filter some?)
         (sorted-map-by :index))))

(defn close-seg-files [files]
  (-> files
      (map #(.close (:fd %)))
      (dorun)))

(defn store-next-index [store]
  )

(defn append-entry [store entry-data]
  )

(extend-protocol p/LogStore
  SegmentLog
  (open! [this]
    (let [logs (load-seg-directory (:directory (.-config this)))]
      (swap! (.-state this) assoc
             :files logs
             :open? true)))

  (write! [this entry])

  (write! [this entry index])

  (reset-index! [this index])

  (flush! [this])

  (read [this])

  (close! [this]
    (close-seg-files (:files @(.-state this)))
    (swap! (.-state this) assoc :open? false)))
