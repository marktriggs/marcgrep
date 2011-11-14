(ns marcgrep.destinations.marcfile
  (:use marcgrep.protocols
        clojure.java.io)
  (:refer-clojure :exclude [next flush])
  (:import [org.marc4j MarcStreamWriter]
           [org.marc4j.marc Record]
           [java.io FileOutputStream]))


(deftype MarcFileDestination [^MarcStreamWriter writer]
  MarcDestination
  (init [this])
  (write [this record]
    (.write writer ^Record record))
  (flush [this])
  (close [this] (.close writer)))


(defn get-output-for [config job]
  (file (:output-dir @config)
        (str (:listen-port @config) "_" (:id @job) ".txt")))


(defn get-destination-for [config job]
  (let [outfile (get-output-for config job)
        outfh (MarcStreamWriter. (FileOutputStream. outfile) "UTF-8")]
    (.deleteOnExit outfile)
    (MarcFileDestination. outfh)))

(marcgrep.core/register-destination
 {:description "MARC file"
  :get-destination-for get-destination-for
  :get-output-for get-output-for
  :required-fields []})
