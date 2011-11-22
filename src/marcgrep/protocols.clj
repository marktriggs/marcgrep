(ns marcgrep.protocols
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


(defprotocol MarcDestination
  "Implementations of this protocol provide a sink for MARC records that
have matched the user's query."

  (init [this]
    "Called once after the MarcDestination is created.  Does whatever
is required to initialise this MARC destination.")

  (write [this record]
    "Called for each record that matched a user's query, and is passed
the matching marc4j Record object.")

  (flush [this]
    "If possible, this should flush any records that have been written
so far to make them available for reading.")

  (close [this]
    "Called when we are done with this MarcDestination.  Should clean up
and close any resources."))
