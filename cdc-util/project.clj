(defproject lymingtonprecision/cdc-util "1.3.0-SNAPSHOT"
  :description "LPE Change Data Capture utility library"
  :url "https://github.com/lymingtonprecision/change-data-capture/cdc-util"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.374"]

                 ;; system
                 [com.stuartsierra/component "0.3.1"]
                 [clj-time "0.11.0"]

                 ;; schema
                 [prismatic/schema "1.0.4"]

                 ;; logging
                 [org.clojure/tools.logging "0.3.1"]

                 ;; database
                 [hikari-cp "1.5.0"]
                 [org.clojars.zentrope/ojdbc "11.2.0.3.0"]

                 ;; (de-)serialization
                 [cheshire "5.5.0"]

                 ;; kafka
                 [clj-kafka "0.3.4"]
                 [org.clojars.ah45/clj-kafka-util "0.1.1"]]

  :profiles {:repl {:source-paths ["dev"]}
             :dev {:dependencies [[org.clojure/test.check "0.9.0"]
                                  [com.gfredericks/test.chuck "0.2.4"]]}}

  :plugins [[lein-codox "0.9.0"]]

  :codox {:metadata {:doc/format :markdown}
          :output-path "../doc/cdc-util"
          :source-uri "https://github.com/lymingtonprecision/change-data-capture/blob/master/cdc-util/{filepath}#L{line}"})
