(ns marcgrep.sources.marcxml-file
  (:refer-clojure :exclude [next])
  (:require marcgrep.sources.concat
            marcgrep.sources.dedupe
            [marcgrep.sources.util :as util]
            [marcgrep.protocols.marc-source :as marc-source])
  (:use clojure.java.io)
  (:import [org.marc4j MarcXmlReader]
           [java.io FileInputStream]
           [org.marc4j.marc Record VariableField]
           [java.io BufferedReader ByteArrayInputStream]))

(deftype MARCXMLFile [^String filename
                      ^{:unsynchronized-mutable true :tag MarcXmlReader} rdr]
  marc-source/MarcSource
  (init [this]
    (set! rdr (MarcXmlReader. (FileInputStream. filename))))
  (next [this]
    (when (.hasNext rdr)
      (.next rdr)))
  (close [this]))


(defn all-marc-records [config]
  (let [marc-source
        (marcgrep.sources.dedupe/dedupe-source
         (apply marcgrep.sources.concat/concat-sources
                (map #(MARCXMLFile. % nil)
                     (util/sort-newest-to-oldest
                      (mapcat util/expand-file-name
                              (:marc-files config))))))]
    (.init marc-source)
    marc-source))
