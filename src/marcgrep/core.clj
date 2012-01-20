(ns marcgrep.core
  (:refer-clojure :exclude [next flush])
  (:use marcgrep.config
        marcgrep.protocols
        compojure.core
        [clojure.contrib.seq :only [positions]]
        [clojure.string :only [join]]
        clojure.java.io
        clojure.contrib.json)
  (:import [org.marc4j MarcXmlReader]
           [org.marc4j.marc Record VariableField DataField ControlField
            Subfield]
           [java.io BufferedReader ByteArrayInputStream FileOutputStream]
           [java.util Date]
           [java.security MessageDigest]
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

(def predicates {"contains_keyword" predicates/contains-keyword?
                 "does_not_contain_keyword" predicates/does-not-contain-keyword?
                 "contains" predicates/contains?
                 "does_not_contain" predicates/does-not-contain?
                 "equals" predicates/equals?
                 "not_equals" predicates/not-equals?
                 "exists" predicates/exists?
                 "does_not_exist" predicates/not-exists?
                 "matches_regexp" [predicates/matches?
                                   {:case-sensitive true}]
                 "does_not_match_regexp" [predicates/not-matches?
                                          {:case-sensitive true}]
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
                   {:tag s :ind1 nil :ind2 nil :subfields nil})
        parsed (update-in parsed [:tag] #(.replace ^String % "*" ""))]
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

    (let [[predicate options] (if (vector? (predicates (:operator query)))
                                (predicates (:operator query))
                                [(predicates (:operator query))])
          value (if (or (not (string? (:value query)))
                        (:case-sensitive options))
                  (:value query)
                  (.toLowerCase ^String (:value query)))
          fieldspec (parse-marc-field (:field query))]
      (fn [record]
        (predicate record fieldspec value)))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Load and save the job queue
;;;

(defmethod print-dup java.util.Date [o w]
  (print-ctor o (fn [o w]
                  (print-dup (.getTime  o) w))
              w))


(defn serialise-job-queue [job-queue]
  (binding [*print-dup* true]
    (prn-str
     (vec (map (fn [job]
                 (dissoc (assoc @job
                           :destination (or (first (positions #{(:destination @job)}
                                                              @marcgrep.core/destinations))
                                            (throw (Exception. (str "Couldn't find destination for "
                                                                    @job)))))
                         :query))
               @job-queue)))))


(defn deserialise-job-queue [s]
  (binding [*print-dup* true]
    (let [queue (read-string s)]
      (vec (map (fn [job]
                  (let [job (assoc job
                              :destination (nth @marcgrep.core/destinations
                                                (:destination job))
                              :query (marcgrep.core/compile-query
                                      (:query-string job)))]
                    (atom
                     (if (= (:status job) :completed)
                       job
                       (assoc job
                         :status :not-started
                         :hits 0
                         :records-checked 0
                         :file-ready? false)))))
                queue)))))

(defn snapshot-job-queue [& [key reference old-state new-state]]
  (let [out-file (:state-file @config)]
    (spit (file (str out-file ".tmp"))
          (serialise-job-queue job-queue))
    (.renameTo (file (str out-file ".tmp"))
               (file out-file))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Job control
;;;


(def job-runner
  "An agent that manages the job run queue."
  (agent []))


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
           (when-let [jobs (seq (filter (fn [job] (not= (:status @job)
                                                        :deleted))
                                        jobs))]
             (doseq [job jobs]
               (swap! job update-in [:records-checked] inc)
               (when ((:query @job) record)
                 (swap! job update-in [:hits] inc)
                 (locking (outputs job)
                   (.write ^marcgrep.protocols.MarcDestination (outputs job)
                           record))))
             (recur))))))))


(defn run-jobs
  "Run a list of jobs.  Spread the work across a bunch of workers and take care
of gathering up and collating their results."
  [jobs source]
  (let [outputs (into {} (map (fn [job]
                                [job ((-> @job :destination
                                          :get-destination-for)
                                      config
                                      job)])
                              jobs))
        queue (LinkedBlockingQueue. 512)
        workers (doall (map #(run-worker % jobs outputs queue)
                            (range (:worker-threads @config))))]
    (with-open [^marcgrep.protocols.MarcSource marc-records source]

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
      (swap! job assoc :completion-time (Date.))
      (swap! job assoc :status :completed))

    (snapshot-job-queue)))


(defn list-ready-jobs []
  (seq (filter #(#{:not-started :waiting} (:status @%))
               @job-queue)))


(defn open-marc-source
  "Prepare a MARC source for access.  This can either be the config entry for
the source, or a ready-to-go MarcSource implementation."
  [source]
  (if (satisfies? MarcSource source)
    (do (.init source)
        source)
    ((ns-resolve (:driver source) 'all-marc-records)
     source)))


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
     (Thread/sleep (:poll-delay-ms @config)))

   ;; To schedule the next batch of jobs, we pick the first job that is ready to
   ;; run, plus all other jobs that will search the same source.
   (when-let [jobs-ready-to-run (list-ready-jobs)]
     (let [next-source-to-run (:source @(first jobs-ready-to-run))
           jobs-to-run (filter #(= (:source @%) next-source-to-run)
                               jobs-ready-to-run)
           marc-source (open-marc-source next-source-to-run)]

       (doseq [job jobs-to-run]
         (swap! job assoc :status :running)
         (swap! job assoc :start-time (Date.)))

       ;; schedule another run to catch any remaining jobs that we haven't run
       ;; in this round (if any).
       (send-off *agent* schedule-job-run)

       ;; and add the new thread to the run queue
       (cons (future (print-errors (run-jobs jobs-to-run marc-source)))
             (filter (complement future-done?) current-jobs))))))



(defn sha1 [s]
  (apply str (map #(format "%02x" %)
                  (.digest (MessageDigest/getInstance "SHA1")
                           (.getBytes s "UTF-8")))))


(defn add-job
  "Add a new job to the job queue"
  [source destination query field-options]
  (when query
    (let [job (atom {:query (compile-query query)
                     :source source
                     :destination destination
                     :field-options field-options

                     ;; internal use attributes...
                     :submission-time (Date.)
                     :hits 0
                     :records-checked 0
                     :file nil
                     :file-ready? false
                     :status :not-started
                     :query-string query
                     :id (sha1 (str query (java.util.Date.)))})]
      (swap! job-queue (fn [queue elt] (conj (vec queue) elt))
             job)
      job)))


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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Web UI
;;;


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
                                  :submission-time (str (:submission-time @job))
                                  :start-time (str (:start-time @job))
                                  :completion-time (str (:completion-time @job))
                                  :status (status-text (:status @job))
                                  :hits (.format (java.text.DecimalFormat.)
                                                 (:hits @job))
                                  :records-checked (.format
                                                    (java.text.DecimalFormat.)
                                                    (:records-checked @job))
                                  :file-available (:file-ready? @job)
                                  :source (:description (:source @job))
                                  :destination (:description
                                                (:destination @job))
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


(defn render-source-options
  "The current list of defined MARC sources in JSON format."
  []
  {:headers {"Content-type" "application/json"}
   :body (json-str (map (fn [source]
                          (into {}
                                (map #(vector % (source %))
                                     [:description])))
                        (:marc-source-list @config)))})


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


(defn int-or-zero [s]
  (try (Integer. s)
       (catch NumberFormatException _ 0)))


(defn handle-add-job
  "Add a new job to the job queue."
  [request]
  (add-job ((:marc-source-list @config)
            (int-or-zero (:source (:params request))))
           (@destinations
            (int-or-zero (:destination
                          (:params request))))
           (read-json (:query (:params request)))
           (read-json (:field-options (:params request))))
  "Added")


(defroutes main-routes
  (POST "/add_job" request (handle-add-job request))
  (POST "/delete_job" request (delete-job (:id (:params request))))
  (POST "/run_jobs" request (do (send-off job-runner #'schedule-job-run)
                                "OK"))
  (GET "/source_options" [id] (render-source-options))
  (GET "/destination_options" [id] (render-destination-options))
  (GET "/job_output/:id" [id] (serve-file id))
  (GET "/job_list" [] (render-job-list))
  (route/files "/" [:root "public"])
  (route/not-found "Page not found"))


(def ^:dynamic *app* (-> #'main-routes params/wrap-params))


(defn -main []
  (load-config-from-file "config.clj")

  (doseq [source (:marc-source-list @config)]
    (require (:driver source)))

  (doseq [destination (:marc-destination-list @config)]
    (require destination))

  (let [state-file (file (:state-file @config))]
    (when (.exists state-file)
      (reset! job-queue (deserialise-job-queue (slurp state-file)))))

  (add-watch job-queue "queue-checkpointer" snapshot-job-queue)

  (jetty/run-jetty (handler/api #'*app*) {:port (:listen-port @config)}))
