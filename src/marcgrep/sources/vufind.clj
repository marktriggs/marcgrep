(ns marcgrep.sources.vufind
  (:refer-clojure :exclude [next])
  (:use marcgrep.protocols
        clojure.java.io)
  (:require marcgrep.sources.lucene))


(defn all-marc-records [config]
  (let [marc-records
        (apply marcgrep.sources.lucene/create-lucene-source
               (:index-path config)
               (:stored-field config)
               :marc
               (flatten (vec config)))]
    (.init marc-records)
    marc-records))
