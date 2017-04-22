(ns kafka-async.commons
  (import java.util.Properties))

(defn map-to-properties
  "Converts a Clojure Map into a `java.util.Properties` object"
  [map]
  (let [props (Properties.)]
  (doseq [entry map]
    (.put props (str (name (key entry))) (val entry)))
  props))

