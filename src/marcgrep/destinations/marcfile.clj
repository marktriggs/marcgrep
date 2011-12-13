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


(defn output-file [config job]
  (file (:output-dir @config)
        (str (:id @job) ".txt")))


(defn get-output-for [config job]
  (let [output (output-file config job)]
    (when (.exists output)
      output)))


(defn get-destination-for [config job]
  (let [outfile (output-file config job)
        outfh (MarcStreamWriter. (FileOutputStream. outfile) "UTF-8")]
    (MarcFileDestination. outfh)))


(defn delete-job [config job]
  (let [output (output-file config job)]
    (when (.exists output)
      (.delete output))))


(marcgrep.core/register-destination
 {:description "MARC file"
  :get-destination-for get-destination-for
  :get-output-for get-output-for
  :delete-job delete-job
  :required-fields []})
