(ns marcgrep.sources.lucene
  (:refer-clojure :exclude [next])
  (:use clojure.java.io)
  (:require [marcgrep.protocols.marc-source :as marc-source]
            [marcgrep.protocols.marc-destination :as marc-destination])
  (:import [org.marc4j MarcReader MarcStreamReader MarcXmlReader]
           [java.io ByteArrayInputStream]
           [java.nio.file Paths]
           [org.apache.lucene.index DirectoryReader IndexReader MultiFields]
           [org.apache.lucene.store FSDirectory]
           [java.util.concurrent LinkedBlockingQueue]))


(defn worker-job [^DirectoryReader dr
                  ^String stored-field
                  marc-flavour
                  preprocess-stored-value-fn
                  batch-size
                  [start-id upper-id]
                  ^LinkedBlockingQueue queue running?]
  (Thread.
   (fn []
     (let [selector #{stored-field}
           live-docs (MultiFields/getLiveDocs dr)]
       (loop [id start-id]
         (when (and @running? (< id upper-id))
           (let [last-id (Math/min (+ id batch-size) upper-id)]
             (let [batch (apply str
                (keep (fn [id]
                  ; https://lucene.apache.org/core/4_0_0/MIGRATE.html LUCENE-2600
                  ; https://lucene.apache.org/core/5_0_0/core/org/apache/lucene/index/IndexReader.html
                  (when (or (nil? live-docs) (.get live-docs id))
                    (preprocess-stored-value-fn
                      (first (.getValues
                       (.document dr id selector)
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
               (recur last-id))
             )
           )))
     (when @running?
       (.put queue :eof)))

   ))


(defn ranges
  "Split the range of numbers from 0-upper into N roughly equal runs."
  [upper n]
  (let [size (quot upper n)]
    (partition 2 1 (concat (butlast (range 0 (inc upper) size))
                           [upper]))))


(deftype LuceneSource [index-path stored-field marc-flavour

                       preprocess-stored-value-fn thread-count batch-size

                       ^{:unsynchronized-mutable true :tag DirectoryReader} dr
                       ^{:unsynchronized-mutable true :tag LinkedBlockingQueue} queue
                       ^{:unsynchronized-mutable true} running?
                       ^{:unsynchronized-mutable true} reader-threads
                       ^{:unsynchronized-mutable true} finished-readers]
  marc-source/MarcSource
  (init [this]
    (set! dr (DirectoryReader/open (FSDirectory/open (Paths/get index-path (into-array String [])))))
    (set! queue (LinkedBlockingQueue. 16))
    (set! running? (atom true))
    (set! finished-readers (atom 0))
    (let [maxdoc (.maxDoc dr)]
      (set! reader-threads
            (doall (map (fn [id-range]
                          (.start (worker-job dr stored-field marc-flavour
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
    (.close dr)))

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
