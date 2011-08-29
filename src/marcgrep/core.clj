(ns marcgrep.core
  (:use compojure.core
        [clojure.string :only [join]]
        clojure.java.io
        clojure.contrib.json)
  (:import [org.marc4j MarcXmlReader MarcStreamWriter MarcXmlWriter]
           [org.marc4j.marc
            Record VariableField DataField
            ControlField Subfield]
           [java.io BufferedReader ByteArrayInputStream FileOutputStream]
           [java.util Date]
           [java.util.concurrent LinkedBlockingQueue])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.params :as params]
            [clojure.contrib.repl-utils :as ru]))


(def job-queue (atom []))
(def output-dir "/var/tmp")
(def worker-threads 2)
(def max-concurrent-jobs 1)

(def controlfield-pattern #"00[0-9]")


(defn matching-fields [^Record record fieldspec]
  (filter (fn [^VariableField field]
            (and
             (= (.getTag field) (:tag fieldspec))
             (or (re-matches controlfield-pattern (:tag fieldspec))
                 (or (not (:ind1 fieldspec))
                     (= (.getIndicator1 ^DataField field)
                        (:ind1 fieldspec)))
                 (or (not (:ind2 fieldspec))
                     (= (.getIndicator2 ^DataField field)
                        (:ind2 fieldspec))))))
          (.getVariableFields record)))




(defn field-values [^Record record fieldspec]
  (if (re-matches controlfield-pattern (:tag fieldspec))
    [(.getData ^ControlField (.getVariableField record (:tag fieldspec)))]
    (when-let [fields (seq (matching-fields record fieldspec))]
      (map (fn [^DataField field]
             (let [subfield-list (if (:subfields fieldspec)
                                   (mapcat #(.getSubfields field %) (:subfields fieldspec))
                                   (.getSubfields field))]
               (join " " (map #(.getData ^Subfield %) subfield-list))))
           fields))))


(defn record-contains? [record fieldspec value]
  (some (fn [^String fv]
          (>= (.indexOf (.toLowerCase fv)
                        value)
              0))
        (field-values record fieldspec)))



(defn record-does-not-contain? [record fieldspec value]
  (when-let [fields (field-values record fieldspec)]
    (not-any? (fn [^String fv]
                (>= (.indexOf (.toLowerCase fv)
                              value)
                    0))
              (field-values record fieldspec))))


(defn record-equals? [record fieldspec value]
  (some (fn [^String fv] (.equalsIgnoreCase fv value))
        (field-values record fieldspec)))

(defn record-not-equals? [record fieldspec value]
  (when-let [fields (field-values record fieldspec)]
    (not-any? (fn [^String fv] (.equalsIgnoreCase fv value))
              (field-values record fieldspec))))



(defn record-exists? [record fieldspec _]
  (field-values record fieldspec))

(def record-not-exists? (complement record-exists?))


(defn record-matches? [record fieldspec value]
  (some (fn [^String fv] (.matches (.matcher (re-pattern value)
                                     (.toLowerCase fv))))
        (field-values record fieldspec)))


(defn record-not-matches? [record fieldspec value]
  (when-let [fields (field-values record fieldspec)]
    (not-any? (fn [^String fv]
                (.matches (.matcher (re-pattern value)
                                    (.toLowerCase fv))))
              fields)))

(defn record-has-repeating-fields? [record fieldspec value]
  (> (count (matching-fields record fieldspec))
     1))

(defn record-does-not-repeat-field? [record fieldspec value]
  (= (count (matching-fields record fieldspec))
     1))


(def predicates {"contains" record-contains?
                 "does_not_contain" record-does-not-contain?
                 "equals" record-equals?
                 "not_equals" record-not-equals?
                 "exists" record-exists?
                 "does_not_exist" record-not-exists?
                 "matches_regexp" record-matches?
                 "does_not_match_regexp" record-not-matches?
                 "repeats_field" record-has-repeating-fields?
                 "does_not_repeat_field" record-does-not-repeat-field?
                 })


(defn parse-marc-field [s]
  (let [parsed (or (when-let [match (re-find #"^([0-9]+)!(.)(.)?\$(.*)" s)]
                     (let [[_ tag ind1 ind2 subfields] match]
                       {:tag tag :ind1 ind1 :ind2 ind2 :subfields subfields}))
                   (when-let [match (re-find #"^([0-9]+)!(.)(.)?$" s)]
                     (let [[_ tag ind1 ind2] match]
                       {:tag tag :ind1 ind1 :ind2 ind2 :subfields nil}))
                   (when-let [match (re-find #"^([0-9]+)\$(.*)$" s)]
                     (let [[_ tag subfields] match]
                       {:tag tag :ind1 nil :ind2 nil :subfields subfields}))
                   {:tag s :ind1 nil :ind2 nil :subfields nil})]
    (reduce (fn [result k]
              (assoc result k
                     (if (= (result k) "#")
                       nil
                       (first (result k)))))
            parsed
            [:ind1 :ind2])))



(defn compile-query [query]
  (if (:boolean query)
    (let [left-fn (compile-query (:left query))
          right-fn (compile-query (:right query))]
      (if (= (:boolean query) "OR")
        (fn [record] (or (left-fn record) (right-fn record)))
        (fn [record] (and (left-fn record) (right-fn record)))))

    (let [value (when (:value query)
                  (.toLowerCase (:value query)))
          fieldspec (parse-marc-field (:field query))]
      (fn [record]
        ((predicates (:operator query))
         record
         fieldspec
         value)))))


(defprotocol MarcSource
  (init [this])
  (next [this])
  (close [this]))

(defn filter-nla-record [^Record record]
  (doseq [vf (.getVariableFields record)]
    (when (= (.getTag ^VariableField vf) "vuf")
      (.removeVariableField record vf)))
  record)



(deftype MarcXMLFile [^{:unsynchronized-mutable true :tag BufferedReader} fh filename]
  MarcSource
  (init [this] (set! fh (reader filename)) )
  (next [this]
    (loop [line :start]
      (when line
        (if (not= "<record>" line)
          (recur (.readLine fh))
          (let [sb (StringBuilder.)]
            (loop [line line]
              (when line
                (.append sb line)
                (.append sb "\n")
                (if (= "</record>" line)
                  (doto (.next (MarcXmlReader.
                                (ByteArrayInputStream.
                                 (.getBytes (.toString sb) "UTF-8"))))
                    (filter-nla-record)
                    )
                  (recur (.readLine fh))))))))))
  (close [this] (.close fh)))



(defn all-marc-records []
  (let [marc-records (MarcXMLFile. nil "/home/mst/updates.xml")]
    (.init marc-records)
    marc-records))


(defn run-worker [id jobs files queue]
  (future
    (loop []
      (let [record (.take queue)]
        (if (= record :eof)
          (.put queue :eof)
          (do (doseq [job jobs]
                (swap! job update-in [:records-checked] inc)
                (when ((:query @job) record)
                  (swap! job update-in [:hits] inc)
                  (locking (:fh (files job))
                    (.write (:fh (files job)) record))))
              (recur)))))))



(def job-runner (agent []))

(defn run-jobs [jobs]
  (let [files (into {} (map (fn [job]
                              (let [outfile (file output-dir (str (:id @job) ".txt"))
                                    outfh (MarcXmlWriter.
                                           (FileOutputStream. outfile))]
                                (.deleteOnExit outfile)
                                [job {:file outfile
                                      :fh outfh}]))
                            jobs))]

    (let [queue (LinkedBlockingQueue. 512)
          workers (doall
                   (map #(run-worker % jobs files queue)
                        (range worker-threads)))]

      (with-open [marc-records (all-marc-records)]
        (doseq [record (take-while identity (repeatedly #(.next marc-records)))]
          (.put queue record))
        (.put queue :eof))

      (doseq [worker workers] @worker))

    (doseq [job jobs]
      ;; close the output file and make it available
      (.close (:fh (files job)))
      (swap! job assoc
             :file (:file (files job))
             :file-ready? true)

      ;; Mark jobs as completed
      (swap! job assoc :status :completed))))


(defn schedule-job-run [current-jobs]
  (while (>= (count (filter (complement future-done?) current-jobs))
             max-concurrent-jobs)
    ;; sit around and wait for a job to finish
    (Thread/sleep 5000))

  ;; Snapshot the job queue and mark those jobs as running
  (when-let [jobs (seq (filter #(= (:status @%) :not-started)
                               @job-queue))]
    (doseq [job jobs] (swap! job assoc :status :running))

    ;; and add the running job to the run queue
    (cons (future (run-jobs jobs))
          (filter (complement future-done?) current-jobs))))



(defn add-job [query]
  (when query
    (swap! job-queue conj
           (atom {:query (compile-query query)
                  :submission-time (Date.)
                  :hits 0
                  :records-checked 0
                  :file nil
                  :file-ready? false
                  :status :not-started
                  :query-string query
                  :id (str (gensym))})))
  "Added")


(defn get-job [id]
  (some (fn [job] (when (= (:id @job) id)
                    job)) @job-queue))


(defn delete-job [id]
  (let [job (get-job id)]
    (when (:file @job)
      (.delete (:file @job)))

    (swap! job-queue #(remove #{job} %)))
  id)


(def status-text {:not-started "Not started"
                  :running "Running"
                  :completed "Finished!"})

(defn render-job-list []
  {:headers {"Content-type" "application/json"}
   :body (json-str {:jobs (map (fn [job]
                                 {:id (:id @job)
                                  :time (str (:submission-time @job))
                                  :status (status-text (:status @job))
                                  :hits (:hits @job)
                                  :records-checked (:records-checked @job)
                                  :file-available (:file-ready? @job)
                                  :query (:query-string @job)})
                               @job-queue)})})



(defn serve-file [id]
  {:headers {"Content-disposition:" (format "attachment; filename=%s.txt"
                                            id)}
   :body (:file @(get-job id))})


(defroutes main-routes
  (POST "/add_job" request (add-job (read-json (:query (:params request)))))
  (POST "/delete_job" request (delete-job (:id (:params request))))
  (POST "/run_jobs" request (do (send-off job-runner #'schedule-job-run)
                                "OK"))
  (GET "/job_output/:id" [id] (serve-file id))
  (GET "/job_list" [] (render-job-list))
  (route/files "/" [:root "public"])
  (route/not-found "Page not found"))

(def ^:dynamic *app* (-> #'main-routes params/wrap-params))


(def server (jetty/run-jetty (handler/api #'*app*) {:port 9095}))
