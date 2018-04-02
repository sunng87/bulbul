(ns bulbul.protocol)

(defprotocol LogStoreWriter
  "A protocol for writting to log stores."
  (open! [this] "Open the log store, load and hold metadata.")
  (write! [this entry] "Append entry to log store.")
  (truncate! [this index] "Reset current index")
  (flush! [this] "Flush buffer, ensure data written to disk.")
  (close! [this] "Close the log store."))

(defprotocol LogStoreReader
  "A protocol for reading from log stores."
  (open! [this] "Open log store reader")
  (take-log [this n] "take n items from current reader")
  (reset-to! [this n] "reset current index to n")
  (close! [this] "close the reader"))
