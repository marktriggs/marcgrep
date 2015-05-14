(ns marcgrep.protocols.marc-destination
  (:refer-clojure :exclude [next flush]))

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
