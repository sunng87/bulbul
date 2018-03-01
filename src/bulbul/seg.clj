(ns bulbul.seg
  (:require [bulbul.protocol :as p]
            [clojure.java.io :as io])
  (:import [java.io RandomAccessFile]))

(def magic-number (.getBytes "BULBUL_LOG_010" "UTF-8"))
(def version 1)

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
   :max-size (* 5 1024 1024)
   :version version})

(defn segment-log [config]
  (let [state (atom (segment-log-initial-state))
        config (merge (segment-log-default-config) config)]
    (SegmentLog. state config)))

(defn open-segment-file [file]
  (let [raf (RandomAccessFile. file "rw")
        _ (.readFully raf (byte-array (alength magic-number)))
        version (.readInt raf)
        id (.readInt raf)
        index (.readLong raf)
        max-size (.readInt raf)
        max-entry (.readInt raf)]
    {:fd raf
     :meta {:version version
            :max-size max-size
            :max-entry max-entry}
     :index index
     :id id}))

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

    {:fd raf
     :meta {:version version
            :max-size (:max-size config)
            :max-entry (:max-entry config)}
     :index index
     :id id}))
