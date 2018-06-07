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

(defmacro with-test-dir [binding & body]
  (let [dir-symbol (first binding)]
    `(let ~binding
       (try
         ~@body
         (finally
           (delete-dir ~dir-symbol)
           (is (not (.exists (io/file ~dir-symbol)))))))))

(deftest test-seg-writer
  (with-test-dir [dir "target/bulbulwritetest/"]
    (let [bullog (s/segment-log default-codec {:directory dir})]
      (bp/open-writer! bullog)
      (is (.exists (io/file dir)))

      (bp/write! bullog [1 200])
      (is (= 1 (count (.listFiles (io/file dir)))))
      (is (> (.length (io/file dir)) 0))

      (is (= 1 (count (:writer-segs @(.-state bullog)))))
      (is (= (count (:writer-segs @(.-state bullog)))
             (count (:seg-files @(.-state bullog)))))
      (bp/close-writer! bullog))))

(deftest test-seg-writer-create-new-seg
  (with-test-dir [dir "target/bulbultest"]
    (let [bullog (s/segment-log default-codec {:directory dir
                                               :max-entry 2})]
      (bp/open-writer! bullog)

      (doseq [n (range 200 203)]
        (bp/write! bullog [1 n]))

      (is (= 2 (count (.listFiles (io/file dir)))))

      (bp/close-writer! bullog))))
