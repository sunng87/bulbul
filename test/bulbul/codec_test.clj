(ns bulbul.codec-test
  (:refer-clojure :exclude [byte float double])
  (:require [bulbul.codec :as sut]
            [clojure.test :as t :refer :all])
  (:import [java.nio ByteBuffer]))

(deftest test-codecs
  (are [data codec]
      (is (= data (sut/decode codec
                              (.. (sut/encode codec data)
                                  (rewind)
                                  (position sut/record-padding)))))

    (clojure.core/byte 1) (sut/byte)
    1 (sut/int16)
    100 (sut/int32)
    (long 1000) (sut/int64)
    (clojure.core/float 32.455) (sut/float)
    (clojure.core/double 32.455) (sut/double)
    "helloworld" (sut/string :prefix (sut/int16) :encoding :utf-8)
    "link" (sut/string :encoding :utf-8 :delimiter "\r\n")
    :hello (sut/enum (sut/int16) {:hello 1 :world 2})
    [:hello "world"] (sut/header
                      (sut/enum (sut/int16)
                                {:hello 1 :world 2})
                         {:hello (sut/string :encoding :utf-8 :prefix (sut/byte))
                          :world (sut/int16)})
       [1 1 (long 1) "helloworld"] (sut/record
                                    (sut/byte)
                                    (sut/int16)
                                    (sut/int64)
                                    (sut/string :prefix (sut/int32) :encoding :ascii))
       [1 :hello "yes"] (sut/record
                         (sut/const 1)
                         (sut/const :hello)
                         (sut/const "yes"))
       [[1] [2] [3]] (sut/counted :prefix (sut/int16) :body (sut/record (sut/byte)))))

(deftest test-nil-results
  (let [codec (sut/string :prefix (sut/int32) :encoding :utf-8)
        buffer (ByteBuffer/allocate 60)]
    (.putInt buffer 50)
    (.put buffer (.getBytes "Hello World" "UTF-8"))
    (.position (.rewind buffer) sut/record-padding)

    (is (nil? (sut/decode codec buffer)))))

(deftest test-byte-block
  (let [buffer (ByteBuffer/allocate 9)
        bytes (.getBytes "Hello World" "UTF-8")
        codec (sut/byte-block :prefix (sut/int16))]

    (let [buffer (.. (sut/encode codec bytes buffer)
                     (rewind)
                     (position sut/record-padding))
          bytes (sut/decode codec buffer)]
      (is (= "Hello World" (String. bytes "UTF-8"))))))
