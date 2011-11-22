(ns marcgrep.test.test-utils
  (:refer-clojure :exclude [next flush])
  (:use marcgrep.protocols
        clojure.test)
  (:require marcgrep.core)
  (:import [org.marc4j.marc MarcFactory]))

(defn marc-record [controlfields datafields]
  (let [factory (MarcFactory/newInstance)
        rec (.newRecord factory)]
    (doseq [[field value] controlfields]
      (.addVariableField rec (.newControlField factory field value)))

    (doseq [datafield datafields]
      (let [df (.newDataField factory
                              (:tag datafield)
                              (or (:ind1 datafield) \space)
                              (or (:ind2 datafield) \space))]
        (doseq [[field value] (:subfields datafield)]
          (.addSubfield df (.newSubfield factory field value)))
        (.addVariableField rec df)))

    rec))


(defn canned-marcsource [& records]
  (let [ptr (atom 0)]
    (reify marcgrep.protocols/MarcSource
      (init [this])
      (next [this]
        (when (< @ptr (count records))
          (do (swap! ptr inc)
              (nth records (dec @ptr)))))
      (close [this]))))


(defn checking-marcdest []
  (let [matched-ids (promise)
        matches (atom #{})]
  {:get-destination-for
   (constantly
    (reify marcgrep.protocols/MarcDestination
      (init [this])
      (write [this record]
        (swap! matches conj (-> record
                                (.getVariableField "001")
                                .getData)))
      (close [this]
        (deliver matched-ids @matches))))
   :matched-ids matched-ids}))


(defn the-query [q & kvs]
  (let [opts (apply hash-map kvs)
        destination (checking-marcdest)]

    (marcgrep.core/add-job
     (apply canned-marcsource (:on opts))
     destination
     q
     nil)

    (send-off marcgrep.core/job-runner marcgrep.core/schedule-job-run)

    (let [matches @(:matched-ids destination)]
      (is (every? matches (:matches opts)) "Things match that should match")
      (is (not-any? matches (:does-not-match opts))
          "Things don't match that shouldn't match"))))
