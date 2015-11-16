(ns cdc-init.components.topic-store
  (:require [clojure.core.async :as async]
            [com.stuartsierra.component :as component]
            [clj-kafka.admin :as kafka.admin]
            [clj-kafka.zk :as zk]
            [clj-kafka.new.producer :as kafka]
            [cdc-init.protocols :refer [TopicStore]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants

(def default-topic-options
  {:partitions 1
   :config {"cleanup.policy" "compact"
            "min.cleanable.dirty.ratio" "0.75"}})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(defn backoff-ms
  "Returns the cube of `n` rounded to the nearest 2 seconds, in
  milliseconds."
  [n]
  (if (zero? n)
    0
    (* (max 2 (* 2 (Math/round (/ (Math/pow n 3) 2)))) 1000)))

(defn replication-factor
  "Returns an appropriate replication factor for the given set of
  brokers."
  [brokers]
  (min 3 (count brokers)))

(defn zk-client
  [this]
  (kafka.admin/zk-client (get-in this [:config "zookeeper.connect"])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord KafkaTopicStore [kafka]
  component/Lifecycle
  (start [this] this)
  (stop [this] this)

  TopicStore
  (topic-exists? [this topic]
    (with-open [zk (zk-client kafka)]
      (kafka.admin/topic-exists? zk topic)))
  (create-topic! [this topic]
    (with-open [zk (zk-client kafka)]
      (kafka.admin/create-topic
       zk topic
       (merge
        default-topic-options
        {"replication.factor" (replication-factor (:brokers kafka))}))))
  (clear-topic! [this topic]
    (with-open [zk (zk-client kafka)]
      (kafka.admin/delete-topic zk topic)
      (loop [attempts 0]
        (if (> attempts 10)
          (throw (Exception. (str "timeout waiting for deletion of topic " topic
                                  " during clear-topic! process")))
          (when (contains? (set (zk/topics (:config kafka))) topic)
            (async/<!! (async/timeout (backoff-ms attempts)))
            (recur (inc attempts)))))
      (.create-topic! this topic)))
  (>!topic [this topic value]
    (let [r (kafka/record topic value)]
      (kafka/send (:producer kafka) r)))
  (>!topic [this topic key value]
    (let [r (kafka/record topic key value)]
      (kafka/send (:producer kafka) r))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn new-topic-store
  "Returns a new, un-started, Kafka component.

  Requires an `:env` map containing a `:zookeeper` key/connection
  string value."
  []
  (component/using
   (map->KafkaTopicStore {})
   [:kafka]))
