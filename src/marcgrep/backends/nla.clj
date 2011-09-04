(ns marcgrep.backends.nla
  (:use marcgrep.protocols
        clojure.java.io)
  (:import [org.marc4j MarcXmlReader]
           [org.marc4j.marc Record VariableField]
           [java.io BufferedReader ByteArrayInputStream]
           [org.apache.lucene.document FieldSelector FieldSelectorResult]
           [org.apache.lucene.index IndexReader]))


(defn filter-nla-record [^Record record]
  (doseq [vf (.getVariableFields record)]
    (when (= (.getTag ^VariableField vf) "vuf")
      (.removeVariableField record vf)))
  record)



(def selector
  (reify FieldSelector
    (accept [this f] (if (= f "fullrecord")
                       FieldSelectorResult/LOAD_AND_BREAK
                       FieldSelectorResult/NO_LOAD))))



(deftype NLASolrRecord [^{:unsynchronized-mutable true :tag IndexReader} ir
                        ^{:unsynchronized-mutable true :tag long} nextdoc
                        ^{:unsynchronized-mutable true :tag String} index-path]
  MarcSource
  (init [this]
    (set! ir (IndexReader/open index-path))
    (set! nextdoc 0))
  (next [this]
    (when (< nextdoc (.maxDoc ir))
      (if (.isDeleted ir nextdoc)
        (do (set! nextdoc (inc nextdoc))
            (recur))
        (let [doc (.document ir nextdoc)]
          (set! nextdoc (inc nextdoc))
          (doto (.next (MarcXmlReader.
                        (ByteArrayInputStream.
                         (.getBytes (first (.getValues doc "fullrecord"))
                                    "UTF-8"))))
            (filter-nla-record))))))
  (close [this] (.close ir)))


(defn all-marc-records [config]
  (let [marc-records (NLASolrRecord. nil 0 (:solr-index @config))]
    (.init marc-records)
    marc-records))
