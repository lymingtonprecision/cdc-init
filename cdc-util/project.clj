(defproject cdc-util "0.1.0-SNAPSHOT"
  :description "LPE Change Data Capture utility library"
  :url "https://github.com/lymingtonprecision/cdc-util"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.371"]

                 ;; system
                 [com.stuartsierra/component "0.3.0"]
                 [clj-time "0.11.0"]

                 ;; database
                 [hikari-cp "1.3.1"]

                 ;; (de-)serialization
                 [cheshire "5.5.0"]

                 ;; kafka
                 [clj-kafka "0.3.2"]
                 [org.clojars.ah45/clj-kafka-util "0.1.0"]]

  :profiles {:repl {:source-paths ["dev"]}
             :dev {:dependencies [[org.clojure/test.check "0.8.2"]
                                  [com.gfredericks/test.chuck "0.2.0"]]}}

  :plugins [[lein-codox "0.9.0"]]

  :codox {:metadata {:doc/format :markdown}
          :output-path "./"
          :source-uri "https://github.com/lymingtonprecision/cdc-util/blob/master/{filepath}#L{line}"})
