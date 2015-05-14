(ns marcgrep.protocols.marc-source
  (:refer-clojure :exclude [next flush]))

(defprotocol MarcSource
  "Implementations of this protocol provide a source of MARC records in
an iterator-style fashion."

  (init [this]
    "Called once after the MarcSource is created.  Does whatever
is required to initialise this MARC source.")

  (next [this]
    "Called each time a new MARC record is required.  Should yield a
marc4j Record object, or nil if no more records are available.")

  (close [this]
    "Called when we are done with this MarcSource.  Should clean up
and close any resources."))
