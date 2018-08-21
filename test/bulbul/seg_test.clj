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
      (bp/close-writer! bullog))))

(deftest test-seg-writer-create-new-seg
  (with-test-dir [dir "target/bulbultest"]
    (let [bullog (s/segment-log default-codec {:directory dir
                                               :max-entry 2})]
      (bp/open-writer! bullog)

      (doseq [n (range 200 203)]
        (bp/write! bullog [1 n]))

      (is (= 2 (count (.listFiles (io/file dir)))))

      ;; internal state
      (is (= 2 (count (:writer-segs @(.-state bullog)))))
      (is (= 0 (:start-index (first (:writer-segs @(.-state bullog))))))
      (is (= 1 @(:last-index (first (:writer-segs @(.-state bullog))))))
      (is (= 2 (:start-index (last (:writer-segs @(.-state bullog))))))
      (is (= 2 @(:last-index (last (:writer-segs @(.-state bullog))))))

      (bp/close-writer! bullog))))

(deftest test-reopen-and-append
  (let [the-dir "target/bulbultest"]
    (let [bullog1 (s/segment-log default-codec {:directory the-dir
                                                :max-entry 2})]
      (bp/open-writer! bullog1)

      (doseq [n (range 200 203)]
        (bp/write! bullog1 [1 n]))

      (bp/close-writer! bullog1))

    (with-test-dir [dir the-dir]
      (let [bullog2 (s/segment-log default-codec {:directory dir
                                                  :max-entry 2})]
        (bp/open-writer! bullog2)

        (is (= 2 (count (:writer-segs @(.-state bullog2)))))
        (is (= 0 (:start-index (first (:writer-segs @(.-state bullog2))))))
        (is (= 1 @(:last-index (first (:writer-segs @(.-state bullog2))))))
        (is (= 2 (:start-index (last (:writer-segs @(.-state bullog2))))))
        (is (= 2 @(:last-index (last (:writer-segs @(.-state bullog2))))))

        (bp/close-writer! bullog2)))))

(deftest test-truncate
  (testing "truncate writer to some position in current file"
    (let [the-dir "target/bulbultest"]
      (with-test-dir [dir the-dir]
        (let [bullog1 (s/segment-log default-codec {:directory dir
                                                    :max-entry 10})]
          (bp/open-writer! bullog1)

          (doseq [n (range 200 205)]
            (bp/write! bullog1 [1 n]))

          (bp/truncate! bullog1 2)

          (is (= 1 @(:last-index (last (:writer-segs @(.-state bullog1))))))

          (bp/write! bullog1 [1 399])

          (is (= 2 @(:last-index (last (:writer-segs @(.-state bullog1))))))))))

  (testing "truncate writer to some position in previous file"
    (let [the-dir "target/bulbultest"]
      (with-test-dir [dir the-dir]
        (let [bullog1 (s/segment-log default-codec {:directory dir
                                                    :max-entry 10})]
          (bp/open-writer! bullog1)

          (doseq [n (range 200 233)]
            (bp/write! bullog1 [1 n]))

          (is (= 32 @(:last-index (last (:writer-segs @(.-state bullog1))))))
          (is (= 4 (count (:writer-segs @(.-state bullog1)))))

          (bp/truncate! bullog1 15)

          (is (= 14 @(:last-index (last (:writer-segs @(.-state bullog1))))))
          (is (= 2 (count (:writer-segs @(.-state bullog1)))))

          (bp/write! bullog1 [1 399])

          (is (= 15 @(:last-index (last (:writer-segs @(.-state bullog1)))))))))))
