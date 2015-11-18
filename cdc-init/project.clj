(defproject cdc-init "1.0.1-SNAPSHOT"
  :description "LPE Change Data Capture initialization service"
  :url "https://github.com/lymingtonprecision/cdc-init"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.371"]

                 ;; system
                 [com.stuartsierra/component "0.3.0"]
                 [environ "1.0.1"]
                 [clj-time "0.11.0"]

                 ;; logging
                 [org.clojure/tools.logging "0.3.1"]
                 [clj-logging-config "1.9.12"]

                 ;; database
                 [org.clojure/java.jdbc "0.4.2"]
                 [org.clojars.zentrope/ojdbc "11.2.0.3.0"]
                 [hikari-cp "1.3.1"]
                 [yesql "0.5.1"]

                 ;; (de-)serialization
                 [cheshire "5.5.0"]

                 ;; kafka
                 [clj-kafka "0.3.2"]
                 [org.clojars.ah45/clj-kafka-util "0.1.1"]]

  :source-paths ["src" "../cdc-util/src"]

  :main cdc-init.main
  :aot [cdc-init.main]

  :uberjar-name "cdc-init-standalone.jar"

  :profiles {:repl {:source-paths ["dev"]}
             :dev {:source-paths ["../cdc-util/test"]
                   :dependencies [[reloaded.repl "0.2.1"]
                                  [org.clojure/test.check "0.8.2"]
                                  [com.gfredericks/test.chuck "0.2.0"]]}}

  :repl-options {:init-ns user :init (reloaded.repl/init)}

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version"
                   "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]])