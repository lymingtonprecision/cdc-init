(ns cdc-util.kafka
  (:require [clojure.tools.logging :as log]

            [clj-kafka.admin :as kafka.admin]
            [clj-kafka.new.producer :as kafka]
            [clj-kafka.zk :as kafka.zk]
            [clj-kafka.consumer.util :as k.c.util]

            [cdc-util.filter :as filter]
            [cdc-util.format :as format]
            [cdc-util.validate :refer [validate-ccd]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(defn reset-offset!
  "Resets the consumer offset on the specified topic of the given
  Kafka consumer group."
  [{zkc "zookeeper.connect" group-id "group.id" :as kafka-config} topic offset]
  (let [get-offset #(kafka.zk/committed-offset kafka-config group-id topic 0)]
    ;; set
    (kafka.zk/set-offset! kafka-config group-id topic 0 offset)
    ;; verify
    (loop [o (get-offset)]
      (when-not (= offset o)
        (recur (get-offset))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(def default-control-topic
  "change-data-capture")

(defn topic-exists?
  "Returns truthy if the specified topic exists within the given Kafka
  configuration."
  [{zkc "zookeeper.connect" :as kafka-config} topic]
  (with-open [zk (kafka.admin/zk-client zkc)]
    (kafka.admin/topic-exists? zk topic)))

(defn create-control-topic!
  "Creates the specified control topic within the given Kafka
  configuration."
  [{zkc "zookeeper.connect" :as kafka-config} topic]
  (let [brokers (kafka.zk/brokers kafka-config)
        replicas (min 3 (count brokers))]
    (log/info "creating control topic" topic)
    (with-open [zk (kafka.admin/zk-client zkc)]
      (kafka.admin/create-topic
       zk topic
       {:partitions 1
        :replication-factor replicas
        :config {"cleanup.policy" "compact"}}))))

(defn topic->last-known-ccd-states
  "Reads all messages posted to the specified control topic and
  returns a collection of the last known states of the CCDs they
  represent.

  Resets the consumer offset for the topic to the maximum offset
  read."
  [kafka-config topic]
  (reset-offset! kafka-config topic 0)
  (let [[ccds max-offset] (filter/topic->ccds-and-max-offset
                           (k.c.util/topic->reduceable
                            topic
                            (assoc kafka-config "consumer.timeout.ms" "5000")
                            (k.c.util/string-decoder)
                            (format/ccd-decoder)))]
    (log/debug "control topic offset set to" (inc max-offset))
    (reset-offset! kafka-config topic (inc max-offset))
    ccds))

(defn ccd-topic-onto-chan
  "Returns a `java.io.Closeable` loop that posts any Change Capture
  Definitions submitted to the specified topic onto the given channel."
  [ch topic kafka-config]
  (k.c.util/topic-onto-chan
   ch
   topic
   kafka-config
   (k.c.util/string-decoder)
   (format/ccd-decoder)))

(defn send-ccd
  "Sends the given Change Capture Definition record to the specified
  topic via the provided Kafka producer.

  Returns the future encapsulating the send op."
  [kafka-producer topic ccd]
  (when-let [json (format/ccd->json-str ccd)]
    (log/debug "send-ccd" (select-keys ccd [:table :status :error :progress]))
    (kafka/send kafka-producer (kafka/record topic (:table ccd) json))))
