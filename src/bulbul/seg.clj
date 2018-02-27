(ns bulbul.seg
  (:require [bulbul.protocol :as p]
            [clojure.java.io :as io])
  (:import [java.io RandomAccessFile]))

(defrecord SegmentLog [state config])

(extend-protocol p/LogStore
  SegmentLog
  (open! [this])

  (write! [this entry])

  (write! [this entry index])

  (reset-index! [this index])

  (flush! [this])

  (read [this])

  (close! [this]) )

(defn segment-log-initial-state []
  {:index 0
   :id 0})

(defn segment-log-default-config []
  {:directory "./bulbul_log"
   :max-entry 1000000
   :max-size (* 5 1024 1024)})

(defn segment-log [config]
  (let [state (atom (segment-log-initial-state))
        config (merge (segment-log-default-config) config)]
    (SegmentLog. state config)))

(defn open-segment-file [file]
  (let [raf (RandomAccessFile. file "rw")
        magic-number (.readLong raf)
        version (.readInt raf)
        index (.readLong raf)
        max-size (.readInt raf)
        max-entry (.readInt raf)]
    {:fd raf
     :meta {:version version
            :max-size max-size
            :max-entry max-entry}
     :index index}))

(defn- segment-file-name [config id]
  (str (:directory config) "/" (:name config) ".log." id))

(defn create-segment-file [id index config]
  (let [raf (RandomAccessFile. (segment-file-name config id))]
    ;; TODO: write header
    ))
