(ns marcgrep.sources.dedupe
  (:refer-clojure :exclude [next])
  (:import [java.util BitSet]
           [org.marc4j.marc Record])
  (:require [marcgrep.protocols.marc-source :as marc-source]))


(deftype Dedupe [marcsource ^{:tag BitSet} seen-ids]
  marc-source/MarcSource
  (init [this] (marc-source/init marcsource))
  (next [this]
    (loop []
      (when-let [^Record next-record (marc-source/next marcsource)]
        (let [control-number (try (Integer/valueOf (.getControlNumber next-record))
                                  (catch Exception _
                                    (.println System/err
                                              (str "WARNING: couldn't parse control number: "
                                                   (.getControlNumber next-record)))
                                    nil))]
          (cond

           ;; Skip this record if we couldn't read its control number or we've
           ;; seen it before
           (or (nil? control-number)
               (.get seen-ids control-number))
           (recur)

           ;; Otherwise, mark as seen and return the record we found
           :else
           (do (.set seen-ids control-number)
               next-record))))))
  (close [this]
    (marc-source/close marcsource)))


(defn dedupe-source [source]
  (Dedupe. source (BitSet.)))

