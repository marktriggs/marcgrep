(ns marcgrep.config
  (:use clojure.java.io)
  (:import [java.io PushbackReader]))

(def config (atom {}))

(defn load-config-from-file [file]
  (reset! config (read (PushbackReader. (reader file)))))
