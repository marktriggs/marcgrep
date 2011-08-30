(defproject marcgrep "0.1"
  :description "A slow-moving search for MARC data"
  :dependencies [[org.clojure/clojure "1.3.0-beta1"]
                 [org.clojure.contrib/complete "1.3.0-SNAPSHOT"]
                 [ring/ring-jetty-adapter "0.3.10"]
                 [compojure "0.6.4"]
                 [org.tigris/marc4j "2.4"]]
  :warn-on-reflection true
  :dev-dependencies [[swank-clojure/swank-clojure "1.3.2"]]
  :main marcgrep.core)
