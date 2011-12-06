(ns marcgrep.sources.util
  (:use clojure.java.io))


(defn expand-file-name [elt]
  (if (map? elt)
    (map #(.getPath %)
         (filter #(re-find (:pattern elt) (.getName %))
                 (.listFiles (file (:dir elt)))))
    [elt]))


(defn sort-newest-to-oldest [paths]
  (sort (fn [a b]
          (compare (.lastModified (file b))
                   (.lastModified (file a))))
        paths))
