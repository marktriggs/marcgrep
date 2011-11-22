(ns marcgrep.sources.marc-file
  (:refer-clojure :exclude [next])
  (:require marcgrep.sources.concat)
  (:use marcgrep.protocols
        clojure.java.io)
  (:import [org.marc4j MarcStreamReader]
           [java.io FileInputStream]
           [org.marc4j.marc Record VariableField]
           [java.io BufferedReader ByteArrayInputStream]))


(deftype MARCFile [^String filename
                   ^{:unsynchronized-mutable true :tag MarcStreamReader} rdr]
  MarcSource
  (init [this]
    (set! rdr (MarcStreamReader. (FileInputStream. filename))))
  (next [this]
    (when (.hasNext rdr)
      (.next rdr)))
  (close [this]))


(defn all-marc-records [config]
  (let [marc-source (apply marcgrep.sources.concat/concat-sources
                           (map #(MARCFile. % nil)
                                (:marc-files config)))]
    (.init marc-source)
    marc-source))
