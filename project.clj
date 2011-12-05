(defproject marcgrep "1.0.0"
  :description "A slow-moving search for MARC data"
  :dependencies [[org.clojure/clojure "1.3.0-beta1"]
                 [org.clojure.contrib/complete "1.3.0-alpha4"]
                 [org.mortbay.jetty/jetty "6.1.25"]
                 [compojure "0.6.4"]
                 [org.tigris/marc4j "2.4"]
                 [org.apache.lucene/lucene-core "3.5.0"]]
  :warn-on-reflection false
  :dev-dependencies [[swank-clojure/swank-clojure "1.3.2"]
                     [lein-ring "0.4.6"]]

  ;; For producing a war file
  :ring {:handler marcgrep.core/handler
         :init marcgrep.core/init}

  :main marcgrep.core)
