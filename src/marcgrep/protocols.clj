(ns marcgrep.protocols)

(defprotocol MarcSource
  (init [this])
  (next [this])
  (close [this]))


(defprotocol MarcDestination
  (init [this])
  (getInputStream [this jobid])
  (write [this record])
  (close [this]))
