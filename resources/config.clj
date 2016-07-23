{:output-dir "/var/tmp"

 :state-file "marcgrep.dat"

 :listen-port 9095

 ;; Note: the total number of active threads will be (roughly) these two values multiplied.
 :worker-threads 2
 :max-concurrent-jobs 1

 :marc-destination-list [marcgrep.destinations.marcfile marcgrep.destinations.plaintext]

 :marc-source-list [{:description "Some MARCXML record files"
                     :driver marcgrep.sources.marcxml-file
                     :marc-files [
                                  ;; Read a single MARCXML file
                                  "/var/tmp/test-records.xml"

                                  ;; And all the files in a directory matching a regular expression
                                  ;; (e.g. /var/tmp/my.file.123.xml, /var/tmp/my.file.234.xml)
                                  {:dir "/var/tmp"
                                   :pattern #"my\.file\.[0-9]+\.xml$"}
                                 ]}

                    {:description "Some MARC record files"
                     :driver marcgrep.sources.marc-file
                     :marc-files [
                                  ;; Read a single MARC file
                                  "/var/tmp/records.mrc"

                                  ;; And all the files in a directory matching a regular expression
                                  ;; (e.g. /var/tmp/my.file.123.mrc, /var/tmp/my.file.234.mrc)
                                  {:dir "/var/tmp"
                                   :pattern #"my\.file\.[0-9]+\.mrc$"}
                                 ]}

                    {:description "Records stored in a VuFind index"
                     :driver marcgrep.sources.vufind
                     :index-path "/tmp/biblio/index"
                     :stored-field "fullrecord"
                    }
                   ]

 :configure-jetty (fn [server]
                    (let [[connector] (.getConnectors server)]
                      (.setMaxIdleTime connector
                                       5000)))
 }
