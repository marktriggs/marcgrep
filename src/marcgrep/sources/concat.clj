(ns marcgrep.sources.concat
  (:refer-clojure :exclude [next])
  (:use marcgrep.protocols))


(deftype Concat [marc-sources
                 ^{:unsynchronized-mutable true} active-source]
  MarcSource
  (init [this]
    (doseq [source marc-sources] (.init source)))
  (next [this]
    (let [active (nth marc-sources active-source)]
      (or (.next active)
          (do (set! active-source (inc active-source))
              (when (< active-source (count marc-sources))
                (.next this))))))
  (close [this]
    (doseq [source marc-sources] (.close source))))


(defn concat-sources [& sources]
  (if (empty? sources)
    (throw (Exception. "Can't concat zero sources!"))
    (Concat. (vec sources) 0)))

