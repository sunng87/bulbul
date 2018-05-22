(ns bulbul.utils)

(defn normalize-ubyte [b]
  (byte (if (> b 127) (- b 255) b)))

(defn denormalize-ubyte [b]
  (if (>= b 0) b (+ b 255)))
