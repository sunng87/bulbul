(ns bulbul.seg.reader
  (:require [bulbul.seg :as seg]
            [bulbul.codec :as bc]
            [clojure.java.io :as io]
            [clojure.data.avl :as cda]
            [bulbul.protocol :as p])
  (:import [bulbul.seg SegmentLog]))


(defn next-entry-in-seg [seg]
  (bc/unwrap-crc32-block (:fd seg)))

;; (defn- open-next-file! [store reader-index]
;;   (let [new-seg-file (nth (:seg-files @(.-state store)) reader-index)
;;         new-seg (seg/open-segment-file new-seg-file)]
;;     ;; online file, skip verification for now
;;     (swap! (.-state store) update :reader-segs conj new-seg)))

(defn jump-to-next-reader-seg!
  "jump to next segment for reader"
  [store]
  (let [next-reader-index (inc (:current-reader-seg-index @(.-state store)))
        next-seg (-> @(.-state store) :reader-segs (nth next-reader-index))]
    (when (some? next-seg)
      (do
        ;; move cursor
        (reset! (:last-index next-seg) (:start-index next-seg))
        (.position (:fd next-seg) 0)
        (swap! (.-state store) update :current-reader-index inc)))))

(defn read-next-entry [store]
  (let [reader-segs (:reader-segs @(.-state store))
        seg-index (:current-reader-seg-index @(.-state store))
        current-seg (nth reader-segs seg-index)]
    (when current-seg
      (let [next-entry (next-entry-in-seg current-seg)]
        (if (nil? next-entry)
          ;; jump to next seg
          (do
            (jump-to-next-reader-seg! store)
            (read-next-entry store))
          (do
            (swap! (.-state store) update-in
                   [:reader-segs seg-index :last-index]
                   inc)
            next-entry))))))

(extend-protocol p/LogStoreReader
  SegmentLog
  (open-reader! [this]
    (let [segs (seg/load-seg-directory (:directory (.-config this)))]
      (swap! (.-state this) assoc
             :reader-segs segs
             :current-reader-seg-index 0)))

  (take-log [this n]
    (take-while some? (repeatedly n #(read-next-entry this))))

  (reset-to! [this n]
    (let [seg-for-n (cda/nearest (:reader-segs @(.-state this)) <= {:start-index n})]
      (seg/move-to-index! seg-for-n n)))

  (close-reader! [this]
    (seg/close-seg-files! (:reader-segs @(.-state this)))
    (swap! (.-state this) dissoc :reader-segs :current-reader-seg-index)))
