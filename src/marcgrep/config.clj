(ns marcgrep.config
  (:use clojure.java.io)
  (:import [java.io PushbackReader]))

(def config (atom {}))

(def defaults {:worker-threads 2
               :max-concurrent-jobs 1
               :poll-delay-ms 5000})

(defn load-config [rdr]
  (reset! config (read (PushbackReader. rdr)))

  ;; Set the defaults
  (doseq [[k v] defaults]
    (when-not (k @config)
      (.println System/err (format "Warning: using default value (%s) for %s"
                                   v k))
      (swap! config assoc k v))))

(defn load-config-from-file [file]
  (load-config (reader file)))
