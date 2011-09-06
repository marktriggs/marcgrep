(ns marcgrep.backends.nla
  (:use marcgrep.protocols
        clojure.java.io)
  (:import [org.marc4j MarcXmlReader]
           [org.marc4j.marc Record VariableField]
           [java.io BufferedReader ByteArrayInputStream]
           [org.apache.lucene.document FieldSelector FieldSelectorResult]
           [org.apache.lucene.index IndexReader]
           [java.util.concurrent LinkedBlockingQueue]))


(def thread-count 3)
(def batch-size 64)


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


(defn worker-job [^IndexReader ir ids ^LinkedBlockingQueue queue running?]
  (Thread.
   (fn []
     (doseq [batch (partition-all batch-size ids)
             :while @running?]
       (let [in (MarcXmlReader.
                 (ByteArrayInputStream.
                  (.getBytes (str "<collection>"
                                  (apply str (keep (fn [id]
                                                     (when-not (.isDeleted ir id)
                                                       (let [doc (.document ir id)]
                                                         (subs (first (.getValues doc "fullrecord"))
                                                               ;; drops the leading: <?xml version="1.0" encoding="utf-8"?>
                                                               38))))
                                                   batch))
                                  "</collection>")
                             "UTF-8")))]
         (while (.hasNext in)
           (.put queue (-> (.next in)
                           (filter-nla-record))))))
     (when @running?
       (.put queue :eof))
     (.println System/err "NLA record reader finished"))))


(deftype NLASolrRecord [^{:unsynchronized-mutable true :tag IndexReader} ir
                        index-path
                        ^{:unsynchronized-mutable true :tag LinkedBlockingQueue} queue

                        ^{:unsynchronized-mutable true} running?
                        ^{:unsynchronized-mutable true} reader-threads
                        ^{:unsynchronized-mutable true} finished-readers]
  MarcSource
  (init [this]
    (set! ir (IndexReader/open index-path))
    (set! queue (LinkedBlockingQueue. 16))
    (set! running? (atom true))
    (set! finished-readers (atom 0))
    (let [maxdoc (.maxDoc ir)]
      (set! reader-threads
            (doall (map (fn [ids] (.start (worker-job ir ids queue running?)))
                        (partition-all (Math/ceil (/ maxdoc thread-count))
                                       (range 0 maxdoc)))))))
  (next [this]
    (when (< @finished-readers (count reader-threads))
      (let [next-record (.take queue)]
        (if (= next-record :eof)
          (do (swap! finished-readers inc)
              (recur))
          next-record))))
  (close [this]
    (reset! running? false)
    (.clear queue)
    (.close ir)))


(defn all-marc-records [config]
  (let [marc-records (NLASolrRecord. nil
                                     (:solr-index @config)
                                     nil nil nil nil)]
    (.init marc-records)
    marc-records))
