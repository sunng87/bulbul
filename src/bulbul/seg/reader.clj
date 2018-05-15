(ns bulbul.seg.reader
  (:require [bulbul.seg :as seg]
            [bulbul.codec :as bc]
            [clojure.java.io :as io]
            [clojure.data.avl :as cda]
            [bulbul.protocol :as p])
  (:import [bulbul.seg SegmentLog]))


(defn next-entry-in-seg [seg]
  (bc/unwrap-crc32-block (:fd seg)))

(defn jump-to-next-reader-seg!
  "jump to next segment for reader"
  [store]
  (let [reader-index (swap! (.-state store) update :current-reader-seg-index inc)
        next-seg (-> @(.-state store) :reader-segs (nth reader-index))]
    (reset! (:last-index next-seg) (:start-index next-seg))
    (.position (:fd next-seg) 0)))

(defn read-next-entry [store]
  (let [reader-segs (:reader-segs @(.-state store))
        current-seg (nth reader-segs (:current-reader-seg-index @(.-state store)))]
    (when current-seg
      (let [next-entry (next-entry-in-seg current-seg)]
        (if (nil? next-entry)
          ;; jump to next seg
          (do
            (jump-to-next-reader-seg! store)
            (read-next-entry store))
          (do
            (swap! (.-state store) update-in
                   [:reader-segs :current-reader-seg-index :last-index]
                   inc)
            next-entry))))))

(extend-protocol p/LogStoreReader
  SegmentLog
  (open-reader! [this]
    (let [logs (seg/load-seg-directory (:directory (.-config this)))]
      (swap! (.-state this) assoc
             :reader-segs logs :current-reader-seg-index 0)))

  (take-log [this n]
    (take-while some? (repeatedly n #(read-next-entry this))))

  (reset-to! [this n]
    (let [seg-for-n (cda/nearest (:reader-segs @(.-state this)) <= {:start-index n})]
      (seg/move-to-index! seg-for-n n)))

  (close-reader! [this]
    (seg/close-seg-files! (:reader-segs @(.-state this)))))
