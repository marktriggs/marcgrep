(ns marcgrep.test.destinations.marcfile
  (:refer-clojure :exclude [next])
  (:use marcgrep.test.test-utils
        clojure.test)
  (:require marcgrep.destinations.marcfile)
  (:import [org.marc4j.marc MarcFactory]))


(def utf8-string (String. (byte-array (map byte [-26 -79 -97 -26 -66
                                                 -92 32 -27 -115 -77
                                                 -27 -65 -125 46]))
                          "UTF-8"))


(def utf8-record
  (marc-record {"001" "12345"}
               [{:tag "245" :subfields [[\a utf8-string]]}]))


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

(deftest utf8-chars-are-not-mangled
  (let [config (atom {:output-dir (System/getProperty "java.io.tmpdir")})
        job (atom {:id (gensym)})]
    (with-open [marcfile (marcgrep.destinations.marcfile/get-destination-for
                          config job)]
      (.write marcfile utf8-record))

    (is (>= (.indexOf (slurp (marcgrep.destinations.marcfile/get-output-for
                              config job))
                      utf8-string)
            0)
        "UTF-8 chars not mangled.")))
