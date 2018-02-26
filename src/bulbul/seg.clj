(ns bulbul.seg
  (:require [bulbul.protocol :as p]))

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
  {:index 0})

(defn segment-log-default-config []
  {:directory "./bulbul_log"
   :max-entry 1E6
   :max-size (* 5 1024 1024)})

(defn segment-log [config]
  (let [state (atom (segment-log-initial-state))
        config (merge (segment-log-default-config) config)]
    (SegmentLog. state config)))

(defn open-segment-file [file]
  )
