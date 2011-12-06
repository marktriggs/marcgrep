(ns marcgrep.sources.remove-deletes
  (:refer-clojure :exclude [next])
  (:import [java.util BitSet]
           [org.marc4j.marc Record])
  (:use marcgrep.protocols))


(deftype RemoveDeletes [^{:tag marcgrep.protocols.MarcSource} marc-source
                        ^{:tag BitSet} deleted-ids]
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
          (if (and control-number
                   (.get deleted-ids control-number))
            (do (println "Dropping delete: " control-number) (recur)) ; skip this record
            next-record)))))
  (close [this] (.close marc-source)))


(defn remove-deletes
  "Drop the records from `source' that have an ID in common with a record in `deletes-marc-source'"
  [^{:tag marcgrep.protocols.MarcSource} source ^marcgrep.protocols.MarcSource deletes-marc-source]
  (let [deleted-ids (BitSet.)]
    (loop []
      (when-let [^Record record (.next deletes-marc-source)]
        (.set deleted-ids (Integer/valueOf (.getControlNumber record)))
        (recur)))

    (RemoveDeletes. source deleted-ids)))

