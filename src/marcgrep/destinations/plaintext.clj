(ns marcgrep.destinations.plaintext
  (:use clojure.java.io)
  (:require [marcgrep.protocols.marc-destination :as marc-destination])
  (:refer-clojure :exclude [next flush])
  (:import [org.marc4j.marc Record VariableField DataField ControlField Subfield]
           [java.io BufferedWriter FileOutputStream]))


(def ms-between-flushes 10000)

(defn write-pretty-record [^Record record ^BufferedWriter out included-fields]
  (doseq [^VariableField f (.getVariableFields record)]
    (let [tag (.getTag f)]
      (when (or (not included-fields)
                (included-fields tag))
        (.write out tag)
        (.write out " ")
        (if (instance? DataField f)
          (let [^DataField f f]
            (do (.write out (str (.getIndicator1 f)))
                (.write out " ")
                (.write out (str (.getIndicator2 f)))
                (doseq [^Subfield sf (.getSubfields f)]
                  (.write out " $")
                  (.write out (str (.getCode sf)))
                  (.write out " ")
                  (.write out (.getData sf)))))
          (.write out (.getData ^ControlField f)))
        (.write out "\r\n"))))
  (.write out "\r\n"))


(deftype PlaintextDestination [^BufferedWriter writer included-fields
                               ^{:unsynchronized-mutable true :tag long} last-flush-time]
  marc-destination/MarcDestination
  (init [this])
  (write [this record]
    (write-pretty-record record writer included-fields)
    (let [now (System/currentTimeMillis)]
      (when (> (- now last-flush-time)
               ms-between-flushes)
        (marc-destination/flush this)
        (set! last-flush-time now))))
  (flush [this] (try (.flush writer)
                     (catch java.io.IOException _)))
  (close [this] (.close writer)))


(defn output-file [config job]
  (file (:output-dir @config)
        (str (:id @job) ".txt")))


(defn get-output-for [config job]
  (let [output (output-file config job)]
    (when (.exists output)
      output)))


(defn get-destination-for [config job]
  (let [outfile (output-file config job)
        outfh (writer outfile)]
    (PlaintextDestination. outfh
                           (when-not (empty? (-> @job :field-options :field-list))
                             (set (.split (-> @job :field-options :field-list)
                                          "\\s*,\\s*")))
                           0)))


(defn delete-job [config job]
  (let [output (output-file config job)]
    (when (.exists output)
      (.delete output))))


(marcgrep.core/register-destination
 {:description "Plain text file"
  :get-destination-for get-destination-for
  :get-output-for get-output-for
  :delete-job delete-job
  :required-fields [{:name "field-list"
                     :label "Limit to fields"
                     :caption "Comma separated.  Leave blank for all fields"
                     :type :text}]})


