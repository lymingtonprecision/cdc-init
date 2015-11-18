(ns cdc-util.components.kafka
  "A component for connecting to and interacting with Apache Kafka."
  (:require [com.stuartsierra.component :as component]
            [clj-kafka.zk :as zk]
            [clj-kafka.new.producer :as kafka]
            [cdc-util.env :refer [env->config]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants

(def consumer-group
  "cdc-init")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord Kafka [zookeeper]
  component/Lifecycle
  (start [this]
    (let [zk-connect {"zookeeper.connect" zookeeper}
          brokers (zk/brokers zk-connect)
          config (merge zk-connect
                        {"bootstrap.servers" (zk/broker-list brokers)
                         "group.id" consumer-group
                         "auto.commit.enable" "false"})
          producer (kafka/producer config
                                   (kafka/string-serializer)
                                   (kafka/string-serializer))]
      (assoc this :config config :producer producer)))
  (stop [this]
    (if-let [p (:producer this)]
      (.close p))
    (dissoc this :producer)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn new-kafka
  "Returns a new, un-started, Kafka component that will connect to
  ZooKeeper using the provided connection string."
  [zookeeper]
  (component/using (->Kafka zookeeper) []))
