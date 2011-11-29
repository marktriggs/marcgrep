(ns marcgrep.sources.nla
  (:refer-clojure :exclude [next])
  (:use marcgrep.protocols
        clojure.java.io)
  (:import [org.marc4j.marc Record VariableField])
  (:require marcgrep.sources.lucene))


(defn chop-xml-header
  "drops the leading: <?xml version=\"1.0\" encoding=\"utf-8\"?>"
  [^String s]
  (subs s 38))


(defn filter-nla-record [^Record record]
  (doseq [vf (.getVariableFields record)]
    (when (= (.getTag ^VariableField vf) "vuf")
      (.removeVariableField record vf)))
  record)


(defn all-marc-records [config]
  (let [^marcgrep.protocols.MarcSource
        marc-records
        (apply marcgrep.sources.lucene/create-lucene-source
               (:index-path config)
               (:stored-field config)
               :xml
               :preprocess-stored-value-fn chop-xml-header
               (flatten (vec config)))]
    (.init marc-records)

    (reify MarcSource
      (init [this])
      (next [this]
        (when-let [rec (.next marc-records)]
          (filter-nla-record rec)))
      (close [this]
        (.close marc-records)))))
