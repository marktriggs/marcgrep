(ns marcgrep.core
  (:use marcgrep.config
        marcgrep.protocols
        compojure.core
        [clojure.string :only [join]]
        clojure.java.io
        clojure.contrib.json)
  (:import [org.marc4j MarcXmlReader]
           [org.marc4j.marc Record VariableField DataField ControlField
            Subfield]
           [java.io BufferedReader ByteArrayInputStream FileOutputStream]
           [java.util Date]
           [java.util.concurrent LinkedBlockingQueue])
  (:require [marcgrep.predicates :as predicates]
            [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.params :as params]
            [clojure.contrib.repl-utils :as ru])
  (:gen-class))


;;; The list of currently known jobs
(def job-queue (atom []))

;;; The registered output plugins.
(def destinations (atom []))


(defn register-destination [opts]
  (swap! destinations conj opts))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Utilities
;;;

(defmacro print-errors [& body]
  `(try ~@body
        (catch Throwable ex#
          (.printStackTrace ex#))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Query parsing
;;;

(def predicates {"contains" predicates/contains?
                 "does_not_contain" predicates/does-not-contain?
                 "equals" predicates/equals?
                 "not_equals" predicates/not-equals?
                 "exists" predicates/exists?
                 "does_not_exist" predicates/not-exists?
                 "matches_regexp" predicates/matches?
                 "does_not_match_regexp" predicates/not-matches?
                 "repeats_field" predicates/has-repeating-fields?
                 "does_not_repeat_field" predicates/does-not-repeat-field?
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



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Job control
;;;

(def job-runner (agent []))


(defn run-worker
  "Run a thread that polls 'queue' for records to process.  Applies each job's
predicate to each record and sends matches to the appropriate destination."
  [id jobs outputs ^LinkedBlockingQueue queue]
  (future
    (print-errors
     (loop []
       (let [record (.take queue)]
         (if (= record :eof)
           ;; Finished.  Push the :eof back for other workers to see.
           (.put queue :eof)
           (when-let [jobs (seq (filter (fn [job] (not= (:status @job) :deleted))
                                        jobs))]
             (doseq [job jobs]
               (swap! job update-in [:records-checked] inc)
               (when ((:query @job) record)
                 (swap! job update-in [:hits] inc)
                 (locking (outputs job)
                   (.write ^marcgrep.protocols.MarcDestination (outputs job) record))))
             (recur))))))))


(defn run-jobs
  "Run a list of jobs.  Spread the work across a bunch of workers and take care
of gathering up and collating their results."
  [jobs]
  (let [outputs (into {} (map (fn [job]
                                [job ((-> @job :destination
                                          :get-destination-for)
                                      config
                                      job)])
                              jobs))
        queue (LinkedBlockingQueue. 512)
        workers (doall (map #(run-worker % jobs outputs queue)
                            (range (:worker-threads @config))))]
    (with-open [^marcgrep.protocols.MarcSource marc-records ((ns-resolve (:marc-backend @config )
                                                      'all-marc-records)
                                          config)]

      ;; Push records onto our queue until we either run out of records or all
      ;; of our workers have called it quits.
      (loop []
        (if (every? future-done? workers)
          nil                           ; slackers!
          (if-let [record (.next marc-records)]
            (do (.put queue record)
                (recur))
            (.put queue :eof)))))

    (doseq [worker workers] @worker)

    (doseq [job jobs]
      ;; close the output file and make it available
      (.close (outputs job))
      (swap! job assoc :file-ready? true)

      ;; Mark jobs as completed
      (swap! job assoc :status :completed))))


(defn schedule-job-run
  "Register our interest in running the current job queue.  If we're already
running as many jobs as we're allowed, wait for an existing run to finish."
  [current-jobs]

  (print-errors
   (while (>= (count (filter (complement future-done?) current-jobs))
              (:max-concurrent-jobs @config))
     ;; sit around and wait for a job to finish
     (doseq [job (filter #(= (:status @%) :not-started) @job-queue)]
       (swap! job assoc :status :waiting))
     (Thread/sleep 5000))

   ;; Snapshot the job queue and mark those jobs as running
   (when-let [jobs (seq (filter #(#{:not-started :waiting} (:status @%))
                                @job-queue))]
     (doseq [job jobs] (swap! job assoc :status :running))

     ;; and add the running job to the run queue
     (cons (future (print-errors (run-jobs jobs)))
           (filter (complement future-done?) current-jobs)))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Web UI
;;;

(defn add-job
  "Add a new job to the job queue"
  [query destination field-options]
  (when query
    (swap! job-queue conj
           (atom {:query (compile-query query)
                  :destination destination
                  :submission-time (Date.)
                  :hits 0
                  :records-checked 0
                  :file nil
                  :file-ready? false
                  :status :not-started
                  :query-string query
                  :field-options field-options
                  :id (str (gensym))})))
  "Added")


(defn get-job
  "Return the job whose ID is 'id'"
  [id]
  (some (fn [job] (when (= (:id @job) id)
                    job))
        @job-queue))


(defn delete-job
  "Delete the job whose ID is 'id'"
  [id]
  (let [job (get-job id)]
    (when (:file @job)
      (.delete (:file @job)))

    (swap! job assoc :status :deleted)
    (swap! job-queue #(remove #{job} %)))
  id)


;;; Mapping from our various statuses to descriptive text.
(def status-text {:not-started "Not started"
                  :waiting "Waiting to run"
                  :running "Running"
                  :completed "Finished!"})



(defn render-job-list
  "The current job list in JSON format"
  []
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


(defn serve-file
  "Handle a request for the output file of job 'id'."
  [id]
  (let [job (get-job id)]
    {:headers {"Content-disposition" (format "attachment; filename=%s.txt"
                                             id)}
     :body ((-> @job :destination :get-output-for)
            config
            job)}))


(defn render-destination-options
  "The current list of output options in JSON format."
  []
  {:headers {"Content-type" "application/json"}
   :body (json-str (map (fn [destination]
                          (into {}
                                (map #(vector % (destination %))
                                     [:description
                                      :required-fields])))
                        @destinations))})


(defn handle-add-job
  "Add a new job to the job queue."
  [request]
  (add-job (read-json (:query (:params request)))
           (try (@destinations
                 (Integer. (:destination
                            (:params request))))
                (catch NumberFormatException _ 0))
           (read-json (:field-options (:params request)))))


(defroutes main-routes
  (POST "/add_job" request (handle-add-job request))
  (POST "/delete_job" request (delete-job (:id (:params request))))
  (POST "/run_jobs" request (do (send-off job-runner #'schedule-job-run)
                                "OK"))
  (GET "/destination_options" [id] (render-destination-options))
  (GET "/job_output/:id" [id] (serve-file id))
  (GET "/job_list" [] (render-job-list))
  (route/files "/" [:root "public"])
  (route/not-found "Page not found"))


(def ^:dynamic *app* (-> #'main-routes params/wrap-params))


(defn -main []
  (load-config-from-file "config.clj")

  (require (:marc-backend @config))
  (doseq [destination (:marc-destination-list @config)]
    (require destination))

  (jetty/run-jetty (handler/api #'*app*) {:port (:listen-port @config)}))
