(ns bulbul.seg
  (:require [bulbul.protocol :as p]
            [clojure.java.io :as io])
  (:import [java.io RandomAccessFile]))

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

(defn open-segment-file [file]
  (let [raf (RandomAccessFile. file "rw")
        magic-number-array (byte-array (alength magic-number))]
    (.readFully raf magic-number-array)
    (when (= (seq magic-number) (seq magic-number-array))
      (let [version (.readInt raf)
            id (.readInt raf)
            index (.readLong raf)
            max-size (.readInt raf)
            max-entry (.readInt raf)]
        (.skipBytes raf header-retain-size)
        {:fd raf
         :meta {:version version
                :max-size max-size
                :max-entry max-entry}
         :index index
         :id id}))))

(defn- segment-file-name [config id]
  (str (:directory config) "/" (:name config) ".log." id))

(defn create-segment-file [id index config]
  (let [raf (RandomAccessFile. (segment-file-name config id) "rw")]
    (.write raf magic-number)
    (.writeInt raf (:version config))
    (.writeInt raf id)
    (.writeLong raf index)
    (.writeInt (:max-size config))
    (.writeInt (:max-entry config))
    (.skipBytes raf header-retain-size)
    {:fd raf
     :meta {:version version
            :max-size (:max-size config)
            :max-entry (:max-entry config)}
     :index index
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
