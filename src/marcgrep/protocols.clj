(ns marcgrep.protocols
  (:refer-clojure :exclude [next flush]))

(defprotocol MarcSource
  (init [this])
  (next [this])
  (close [this]))


(defprotocol MarcDestination
  (init [this])
  (getInputStream [this jobid])
  (write [this record])
  (flush [this])
  (close [this]))
