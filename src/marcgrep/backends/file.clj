(ns marcgrep.backends.file
  (:use [marcgrep.core :only [MarcSource]]
        [clojure.java.io])
  (:import [org.marc4j MarcXmlReader]
           [java.io FileInputStream]
           [org.marc4j.marc Record VariableField]
           [java.io BufferedReader ByteArrayInputStream]))

(deftype MarcFile [^{:unsynchronized-mutable true :tag String} filename
                   ^{:unsynchronized-mutable true :tag MarcXmlReader} rdr]
  MarcSource
  (init [this]
    (set! rdr (MarcXmlReader. (FileInputStream. filename))))
  (next [this]
    (when (.hasNext rdr)
      (.next rdr)))
  (close [this]))


(defn all-marc-records [config]
  (let [marc-records (MarcFile. (:marc-file @config) nil)]
    (.init marc-records)
    marc-records))
