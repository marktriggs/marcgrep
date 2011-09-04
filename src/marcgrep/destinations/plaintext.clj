(ns marcgrep.destinations.plaintext
  (:use marcgrep.protocols
        clojure.java.io)
  (:import [org.marc4j.marc Record VariableField DataField ControlField Subfield]
           [java.io BufferedWriter FileOutputStream]))


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
        (.write out "\n"))))
  (.write out "\n"))


(deftype PlaintextDestination [^{:unsynchronized-mutable true :tag BufferedWriter} writer
                               included-fields]
  MarcDestination
  (init [this])
  (write [this record] (write-pretty-record record writer included-fields))
  (close [this] (.close writer)))


(defn get-output-for [config job]
  (file (:output-dir @config)
        (str (:id @job) ".txt")))


(defn get-destination-for [config job]
  (let [outfile (get-output-for config job)
        outfh (writer outfile)]
    (.deleteOnExit outfile)
    (PlaintextDestination. outfh
                           (when-not (empty? (-> @job :field-options :field-list))
                             (set (.split (-> @job :field-options :field-list)
                                          "\\s*,\\s*"))))))


(marcgrep.core/register-destination
 {:description "Plain text output"
  :get-destination-for get-destination-for
  :get-output-for get-output-for
  :required-fields [{:name "field-list"
                     :label "Limit to fields"
                     :caption "Comma separated.  Leave blank for all fields"
                     :type :text}]})


