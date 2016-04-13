(ns marcgrep.sources.voyager
  (:refer-clojure :exclude [next])
  (:require marcgrep.sources.remove-deletes
            marcgrep.sources.marc-file
            [marcgrep.protocols.marc-source :as marc-source])
  (:use clojure.java.io)
  (:import [org.marc4j MarcStreamReader]
           [java.io FileInputStream]
           [org.marc4j.marc Record VariableField]
           [java.io BufferedReader ByteArrayInputStream]))


(defn all-marc-records [config]
  (let [deletes-source (marcgrep.sources.marc-file/all-marc-records
                        (assoc config
                          :marc-files [(:voyager-deletes-file config)]))
        marc-source
        (marcgrep.sources.remove-deletes/remove-deletes
         (marcgrep.sources.marc-file/all-marc-records config)
         deletes-source)]
    (.close deletes-source)
    marc-source))
