(ns marcgrep.sources.util
  (:use clojure.java.io))


(defn expand-file-name [elt]
  (if (map? elt)
    (map #(.getPath %)
         (filter #(re-find (:pattern elt) (.getName %))
                 (.listFiles (file (:dir elt)))))
    [elt]))
