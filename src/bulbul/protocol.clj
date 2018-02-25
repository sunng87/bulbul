(ns bulbul.protocol
  (:refer-clojure :exclude [read]))

(defprotocol LogStore
  "A protocol for log stores."
  (open! [this] "Open the log store, load and hold metadata.")
  (write! [this entry] "Append entry to log store.")
  (flush! [this] "Flush buffer, ensure data written to disk.")
  (read [this] "Read log store, may return lazy-seq for logs."))
