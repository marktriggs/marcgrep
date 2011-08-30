(ns marcgrep.destinations.marcfile
  (:use [marcgrep.core :only [MarcDestination]]
        [clojure.java.io])
  (:import [org.marc4j MarcStreamWriter]
           [org.marc4j.marc Record]
           [java.io FileOutputStream]))


(deftype MarcFileDestination [^{:unsynchronized-mutable true :tag MarcStreamWriter} writer]
  MarcDestination
  (init [this])
  (write [this record]
    (.write writer ^Record record))
  (close [this] (.close writer)))


(defn get-output-for [config job]
  (file (:output-dir @config)
        (str (:id @job) ".txt")))


(defn get-destination-for [config job]
  (let [outfile (get-output-for config job)
        outfh (MarcStreamWriter. (FileOutputStream. outfile))]
    (.deleteOnExit outfile)
    (MarcFileDestination. outfh)))
