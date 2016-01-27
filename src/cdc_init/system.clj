(ns cdc-init.system
  (:require [com.stuartsierra.component :as component]
            [environ.core :refer [env]]

            [cdc-init.components.change-data-store :refer [new-change-data-store]]
            [cdc-init.components.initializer :refer [new-initializer]]
            [cdc-init.components.seed-store :refer [new-seed-store]]
            [cdc-init.components.topic-store :refer [new-topic-store]]
            [cdc-util.components.database :refer [new-database-from-env]]
            [cdc-util.kafka :refer [default-control-topic]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn new-system
  ([] (new-system env))
  ([env]
   (component/system-map
    :database (new-database-from-env env)
    :change-data-store (new-change-data-store)
    :seed-store (new-seed-store)
    :topic-store (new-topic-store (env :zookeeper))
    :initializer (new-initializer
                  (env :zookeeper)
                  (env :control-topic default-control-topic)))))
