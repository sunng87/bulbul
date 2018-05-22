(ns bulbul.seg-test
  (:require [bulbul.seg :as s]
            [bulbul.protocol :as bp]
            [bulbul.seg.writer :as sw]
            [bulbul.seg.reader :as sr]
            [bulbul.codec :as bc]
            [clojure.test :refer :all]
            [clojure.java.io :as io]))

(def default-codec
  (bc/record
   (bc/byte)
   (bc/int32)))

(defn delete-dir [dir]
  (let [files (file-seq (io/file dir))]
    (doseq [f (reverse files)]
      (io/delete-file f true))))

(deftest test-seg-writer
  (let [dir "target/bulbulwritetest/"]
    (try
      (let [bullog (s/segment-log default-codec {:directory dir})]
        (bp/open-writer! bullog)
        (is (.exists (io/file dir)))

        (bp/write! bullog [1 200])
        (is (= 1 (count (.listFiles (io/file dir)))))
        (is (> (.length (io/file dir)) 0)))
      (finally
        (delete-dir dir)
        (is (not (.exists (io/file dir))))))))
