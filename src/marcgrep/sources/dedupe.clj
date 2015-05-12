(ns marcgrep.sources.dedupe
  (:refer-clojure :exclude [next])
  (:import [java.util BitSet]
           [org.marc4j.marc Record])
  (:use marcgrep.protocols))


(deftype Dedupe [^{:tag marcgrep.protocols.MarcSource} marc-source
                 ^{:tag BitSet} seen-ids]
  MarcSource
  (init [this] (.init marc-source))
  (next [this]
    (loop []
      (when-let [^Record next-record (.next marc-source)]
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
    (.close marc-source)))


(defn dedupe-source [source]
  (Dedupe. source (BitSet.)))

