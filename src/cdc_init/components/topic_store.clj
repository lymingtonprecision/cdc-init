(ns cdc-init.components.topic-store
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
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

(defn topics
  "Returns the set of current topics configured on the specified ZooKeeper
  quorum."
  [zk-connect]
  (set (zk/topics {"zookeeper.connect" zk-connect})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord KafkaTopicStore [zk-connect]
  component/Lifecycle
  (start [this]
    (if (:producer this)
      this
      (let [brokers (zk/brokers {"zookeeper.connect" zk-connect})
            producer (kafka/producer
                      {"bootstrap.servers" (zk/broker-list brokers)}
                      (kafka/string-serializer)
                      (kafka/string-serializer))]
        (assoc this :producer producer))))
  (stop [this]
    (when-let [p (:producer this)]
      (.close p))
    (dissoc this :producer))

  TopicStore
  (topic-exists? [this topic]
    (with-open [zk (kafka.admin/zk-client zk-connect)]
      (kafka.admin/topic-exists? zk topic)))
  (create-topic! [this topic]
    (let [brokers (zk/brokers {"zookeeper.connect" zk-connect})
          opt (merge
               default-topic-options
               {:replication-factor (replication-factor brokers)})]
      (log/debug (str "creating " topic " with") opt)
      (with-open [zk (kafka.admin/zk-client zk-connect)]
        (kafka.admin/create-topic zk topic opt))))
  (clear-topic! [this topic]
    (with-open [zk (kafka.admin/zk-client zk-connect)]
      (kafka.admin/delete-topic zk topic)
      (loop [attempts 0]
        (if (> attempts 10)
          (throw (Exception. (str "timeout waiting for deletion of topic " topic
                                  " during clear-topic! process")))
          (when (contains? (topics zk-connect) topic)
            (async/<!! (async/timeout (backoff-ms attempts)))
            (recur (inc attempts)))))
      (.create-topic! this topic)))
  (>!topic [this topic value]
    (let [r (kafka/record topic value)]
      (kafka/send (:producer this) r)))
  (>!topic [this topic key value]
    (let [r (kafka/record topic key value)]
      (kafka/send (:producer this) r))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn new-topic-store
  "Returns a new, un-started, KafkaTopicStore component.

  Requires a `zk-connect` ZooKeeper connection string."
  [zk-connect]
  (component/using
   (->KafkaTopicStore zk-connect)
   []))
