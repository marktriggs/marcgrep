(ns marcgrep.test.core
  (:refer-clojure :exclude [next])
  (:use marcgrep.protocols
        clojure.test)
  (:require marcgrep.core
            marcgrep.config)
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
      (getInputStream [this id])
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




(def test-dataset
  [(marc-record {"001" "12345"}
                [{:tag "245" :subfields [[\a "hello world"]]}])
   (marc-record {"001" "23456"}
                [{:tag "245" :subfields [[\a "this is a test"]]}])])


(defn suite-setup [f]
  (marcgrep.config/load-config (java.io.StringReader. "{:max-concurrent-jobs 1}"))
  (f))


(defn test-setup [f]
  (reset! marcgrep.core/job-queue [])
  (f))


(use-fixtures :once suite-setup)
(use-fixtures :each test-setup)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests...


(deftest substring-search
  (the-query {:operator "contains"
              :field "245$a"
              :value "hello"}
             :on test-dataset
             :matches ["12345"]
             :does-not-match ["23456"]))


(deftest substring-search-ignores-case
  (the-query {:operator "contains"
              :field "245$a"
              :value "HELLO WoRlD"}
             :on test-dataset
             :matches ["12345"]))

(deftest regexp-search-honours-case
  (the-query {:operator "matches_regexp"
              :field "245$a"
              :value ".*HELLO.*"}
             :on test-dataset
             :does-not-match ["12345"]))


(deftest sources-are-batched
  (let [gen-1 (apply canned-marcsource test-dataset)
        gen-2 (apply canned-marcsource test-dataset)

        dest-1 (checking-marcdest)
        dest-2 (checking-marcdest)
        dest-3 (checking-marcdest)
        dest-4 (checking-marcdest)

        query {:operator "contains"
               :field "245$a"
               :value "hello"}

        ;; In spite of them being added in this order, we should see both the
        ;; gen-1 tests run first.
        job-1 (marcgrep.core/add-job gen-1 dest-1 query nil)
        job-2 (marcgrep.core/add-job gen-2 dest-2 query nil)
        job-3 (marcgrep.core/add-job gen-1 dest-3 query nil)
        job-4 (marcgrep.core/add-job gen-2 dest-4 query nil)

        test-fn (fn [key a old-state new-state]
                  (when (and (not= (:status old-state) :running)
                             (= (:status new-state) :running))
                    ;; If gen-2 jobs are running then both of the gen-1 jobs
                    ;; should have now finished.
                    (is (= (:status @job-1) :completed))
                    (is (= (:status @job-3) :completed))))]

    (add-watch job-2 :ensure-gen-1-jobs-finished test-fn)
    (add-watch job-4 :ensure-gen-1-jobs-finished test-fn)

    (send-off marcgrep.core/job-runner marcgrep.core/schedule-job-run)

    ;; wait for them all to finish
    (doseq [dst [dest-1 dest-2 dest-3 dest-4]]
      @(:matched-ids dst))))

