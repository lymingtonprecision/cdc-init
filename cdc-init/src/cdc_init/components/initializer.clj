(ns cdc-init.components.initializer
  "A component to process Change Data Capture initialization
  requests."
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]

            [clj-kafka.admin :as kafka.admin]
            [clj-kafka.new.producer :as kafka]
            [clj-kafka.zk :as kafka.zk]
            [clj-kafka.consumer.util :as k.c.util]

            [cdc-util.async
             :refer [pipe-ret-last
                     go-till-closed
                     noop-transducer]]
            [cdc-util.filter :as filter]
            [cdc-util.format :as format]
            [cdc-util.validate :refer [validate-ccd]]

            [cdc-init.core :refer :all]
            [cdc-init.protocols :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Kafka/control topic interaction

(defn control-topic-exists?
  "Returns truthy if the required control topic exists within the
  given Kafka configuration."
  [{zkc "zookeeper.connect" :as kafka-config} topic]
  (with-open [zk (kafka.admin/zk-client zkc)]
    (kafka.admin/topic-exists? zk topic)))

(defn create-control-topic!
  "Creates the specified control topic within the given Kafka
  configuration."
  [{zkc "zookeeper.connect" :as kafka-config} topic]
  (let [brokers (-> kafka-config kafka.zk/brokers)
        replicas (min 3 (count brokers))]
    (log/info "creating control topic" topic)
    (with-open [zk (kafka.admin/zk-client zkc)]
      (kafka.admin/create-topic
       zk topic
       {:partitions 1
        :replication-factor replicas
        :config {"cleanup.policy" "compact"}}))))

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
;; Utility fns

(defn ccds-to-initialize
  "Reads all messages posted to the specified control topic and
  returns the subset that represent Change Capture Definitions
  awaiting proper initialization.

  Resets the consumer offset for the topic to the maximum offset
  read."
  [kafka-config topic]
  (reset-offset! kafka-config topic 0)
  (let [[ccds max-offset] (filter/topic->ccds-to-initialize
                           (k.c.util/topic->reduceable
                            topic
                            (assoc kafka-config "consumer.timeout.ms" "5000")
                            (k.c.util/string-decoder)
                            (format/ccd-decoder)))]
    (log/debug "control topic offset set to" (inc max-offset))
    (reset-offset! kafka-config topic (inc max-offset))
    ccds))

(defn send-ccd
  "Sends the given Change Capture Definition record to the specified
  topic via the provided Kafka producer.

  Returns the future encapsulating the send op."
  [kafka-producer topic ccd]
  (if-let [json (some-> ccd validate-ccd format/ccd->json-str)]
    (do
      (log/debug "send-ccd" (select-keys ccd [:table :status :error :progress]))
      (kafka/send kafka-producer (kafka/record topic (:table ccd) json)))
    (log/error "send-ccd failed: invalid CCD record" ccd)))

(defn updates-chan
  "Returns a channel that will post any received Change Capture
  Definition updates to the specified topic via the given Kafka
  producer."
  [kafka-producer topic]
  (async/chan 1 (noop-transducer (partial send-ccd kafka-producer topic))))

(defn submissions-onto-chan
  "Returns a `java.io.Closeable` loop that posts any Change Capture
  Definitions submitted to the specified topic onto the given channel."
  [ch topic kafka-config]
  (k.c.util/topic-onto-chan
   ch
   topic
   kafka-config
   (k.c.util/string-decoder)
   (format/ccd-decoder)))

(defn initialize-ccd-loop
  "Returns a `java.io.Closeable` loop that takes Change Capture
  Definitions posted to the given channel and runs them through the
  Change Data Capture initialization process, posting any generated
  status updates to `updates-ch`."
  [ch updates-ch change-data-store seed-store topic-store]
  (go-till-closed
   ch
   (fn [ccd _]
     (log/info "processing CCD for" (:table ccd))
     (some-> ccd
             (prepare change-data-store topic-store)
             (pipe-ret-last updates-ch false)
             async/<!!
             ((fn [ccd]
                (log/info "preparation complete for" (:table ccd) " now seeding")
                ccd))
             (initialize topic-store seed-store change-data-store)
             (pipe-ret-last updates-ch false)
             async/<!!)
     (log/info "finished processing" (:table ccd)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord ChangeCaptureInitializer
    [control-topic kafka change-data-store seed-store topic-store]
  component/Lifecycle
  (start [this]
    (if (:process-loop this)
      this
      (do
        (log/info "starting initializer")
        (when-not (control-topic-exists? (:config kafka) control-topic)
          (create-control-topic! (:config kafka) control-topic))
        (let [ccds (ccds-to-initialize (:config kafka) control-topic)
              _ (log/info "found" (count ccds) "CCDs waiting initializtion")
              ;; create a work queue
              ;; ... and seed it with the definitions awaiting initialization
              queue (async/chan 100)
              _ (async/onto-chan queue (sort-by :timestamp ccds) false)
              ;; create a channel to filter out new submissions from updates
              ;; ... and pipe those new submissions through to the work queue
              submissions (async/chan 1 filter/msgs->submitted-ccds)
              _ (async/pipe submissions queue)
              ;; synchronously post progress updates back to kafka
              updates (updates-chan (:producer kafka) control-topic)]
          ;; setup complete, create our "worker" loops and return
          (let [submission-loop (submissions-onto-chan
                                 submissions
                                 control-topic
                                 (:config kafka))
                process-loop (initialize-ccd-loop
                              queue
                              updates
                              change-data-store
                              seed-store
                              topic-store)]
            (assoc this
                   :queue queue
                   :submission-loop submission-loop
                   :process-loop process-loop))))))
  (stop [this]
    (if (some identity (vals (select-keys this [:process-loop
                                                :submission-loop])))
      (do
        (when-let [l (:submission-loop this)]
          (log/info "terminating submission loop")
          (.close l))
        (when-let [l (:process-loop this)]
          (log/info "terminating process loop")
          (.close l))
        (log/info "stopped initializer")
        (dissoc this :submission-loop :process-loop :queue))
      this)))

(defn new-initializer
  "Returns a new, un-started, Change Data Capture Initializer
  processing definitions posted to the specified Kafka
  `control-topic`.

  Will read and process existing definitions from the topic when
  started and process new submissions as they are posted."
  [control-topic]
  (component/using
   (map->ChangeCaptureInitializer {:control-topic control-topic})
   [:kafka
    :change-data-store
    :seed-store
    :topic-store]))
