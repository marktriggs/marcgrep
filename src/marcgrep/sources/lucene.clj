(ns marcgrep.sources.lucene
  (:refer-clojure :exclude [next])
  (:use clojure.java.io)
  (:require [marcgrep.protocols.marc-source :as marc-source]
            [marcgrep.protocols.marc-destination :as marc-destination])
  (:import [org.marc4j MarcReader MarcStreamReader MarcXmlReader]
           [java.io ByteArrayInputStream]
           [org.apache.lucene.document FieldSelector FieldSelectorResult]
           [org.apache.lucene.index IndexReader]
           [org.apache.lucene.store FSDirectory]
           [java.util.concurrent LinkedBlockingQueue]))


(defn selector-for-field [field]
  (reify FieldSelector
    (accept [this f] (if (= f field)
                       FieldSelectorResult/LOAD_AND_BREAK
                       FieldSelectorResult/NO_LOAD))))


(defn worker-job [^IndexReader ir
                  ^String stored-field
                  marc-flavour
                  preprocess-stored-value-fn
                  batch-size
                  [start-id upper-id]
                  ^LinkedBlockingQueue queue running?]
  (Thread.
   (fn []
     (let [selector (selector-for-field stored-field)]
       (loop [id start-id]
         (when (and @running? (< id upper-id))
           (let [last-id (Math/min (+ id batch-size) upper-id)]
             (let [batch (apply str
                                (keep (fn [id]
                                        (when-not (.isDeleted ir id)
                                          (preprocess-stored-value-fn
                                           (first (.getValues
                                                   (.document ir id selector)
                                                   stored-field)))))
                                      (range id last-id)))
                   batch (if (= marc-flavour :xml)
                                   (str "<collection>" batch "</collection>")
                                   batch)
                   bis (ByteArrayInputStream.
                        (.getBytes ^String batch "UTF-8"))
                   ^MarcReader in (if (= marc-flavour :xml)
                                    (MarcXmlReader. bis)
                                    (MarcStreamReader. bis))]
               (while (.hasNext in)
                 (.put queue (.next in)))
               (recur last-id))))))
     (when @running?
       (.put queue :eof)))))


(defn ranges
  "Split the range of numbers from 0-upper into N roughly equal runs."
  [upper n]
  (let [size (quot upper n)]
    (partition 2 1 (concat (butlast (range 0 (inc upper) size))
                           [upper]))))


(deftype LuceneSource [index-path stored-field marc-flavour

                       preprocess-stored-value-fn thread-count batch-size

                       ^{:unsynchronized-mutable true :tag IndexReader} ir
                       ^{:unsynchronized-mutable true :tag LinkedBlockingQueue} queue
                       ^{:unsynchronized-mutable true} running?
                       ^{:unsynchronized-mutable true} reader-threads
                       ^{:unsynchronized-mutable true} finished-readers]
  marc-source/MarcSource
  (init [this]
    (set! ir (IndexReader/open (FSDirectory/open (file index-path))
                               true))
    (set! queue (LinkedBlockingQueue. 16))
    (set! running? (atom true))
    (set! finished-readers (atom 0))
    (let [maxdoc (.maxDoc ir)]
      (set! reader-threads
            (doall (map (fn [id-range]
                          (.start (worker-job ir stored-field marc-flavour
                                              (or preprocess-stored-value-fn
                                                  identity)
                                              batch-size
                                              id-range queue running?)))
                        (ranges maxdoc thread-count))))))
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


(defn create-lucene-source [index-path stored-field marc-flavour & opts]
  (let [opts (apply hash-map opts)]
    (LuceneSource. index-path stored-field marc-flavour
                   (or (:preprocess-stored-value-fn opts)
                       identity)
                   (or (:parser-threads opts)
                       3)
                   (or (:batch-size opts)
                       64)

                   nil nil nil nil nil)))
