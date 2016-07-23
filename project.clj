(defproject marcgrep "1.0.5"
  :description "A slow-moving search for MARC data"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/data.json "0.2.4"]
                 [org.mortbay.jetty/jetty "6.1.25"]
                 [compojure "0.6.4"]
                 [ring/ring-servlet "0.3.10"]
                 [org.tigris/marc4j "2.4"]
                 [org.apache.lucene/lucene-core "5.5.0"]]
  :plugins [[lein-ring "0.8.5"]]
  :warn-on-reflection false
  :dev-dependencies [[swank-clojure/swank-clojure "1.3.2"]]

  ;; For producing a war file
  :ring {:handler marcgrep.core/handler
         :init marcgrep.core/init}

  :aot :all

  :main marcgrep.core)
