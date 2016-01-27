(ns cdc-init.components.initializer
  "A component to process Change Data Capture initialization
  requests."
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [clj-kafka.new.producer :as kafka.producer]

            [cdc-util.async :refer [pipe-ret-last go-till-closed noop-transducer]]
            [cdc-util.filter :as filter]
            [cdc-util.kafka :as kafka.util]
            [cdc-util.validate :refer [check-ccd]]

            [cdc-init.core :refer :all]))

(def ^:dynamic *consumer-group* "cdc-init")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants

(def non-initializable-statuses
  #{:active :error})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(defn ccds-to-initialize
  "Reads all messages posted to the specified control topic and
  returns the subset that represent Change Capture Definitions
  awaiting proper initialization.

  Resets the consumer offset for the topic to the maximum offset
  read."
  [kafka-config topic]
  (let [ccds (remove
              #(contains? non-initializable-statuses (:status %))
              (kafka.util/topic->last-known-ccd-states kafka-config topic))]
    (log/info "found" (count ccds) "CCDs waiting initializtion")
    ccds))

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
     (if-let [err (check-ccd ccd)]
       (async/>!! updates-ch
                  (update-status
                   ccd :error
                   {:error {:message "invalid specification"
                            :info err}}))
       (some-> ccd
               (prepare change-data-store topic-store)
               (pipe-ret-last updates-ch false)
               async/<!!
               ((fn [ccd]
                  (when (not= :error (:status ccd))
                    (log/info "preparation complete for" (:table ccd) " now seeding")
                    ccd)))
               (initialize topic-store seed-store change-data-store)
               (pipe-ret-last updates-ch false)
               async/<!!))
     (log/info "finished processing" (:table ccd)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord ChangeCaptureInitializer
    [zk-connect control-topic change-data-store seed-store topic-store]
  component/Lifecycle
  (start [this]
    (if (:process-loop this)
      this
      (let [kafka-config
            {"zookeeper.connect" zk-connect
             "bootstrap.servers" (kafka.util/bootstrap-servers zk-connect)
             "group.id" *consumer-group*
             "auto.commit.enable" "false"}
            _ (when-not (kafka.util/topic-exists? kafka-config control-topic)
                (kafka.util/create-control-topic! kafka-config control-topic))
            ccds (ccds-to-initialize kafka-config control-topic)
            queue (async/chan 100)
            submissions (async/chan 1 (filter/msgs->ccds-with-status :submitted))
            ;; synchronously post progress updates back to kafka
            producer (kafka.producer/producer
                      kafka-config
                      (kafka.producer/string-serializer)
                      (kafka.producer/string-serializer))
            send-ccd-update! (partial kafka.util/send-ccd producer control-topic)
            updates-chan (async/chan 1 (noop-transducer send-ccd-update!))]
        ;; seed our work queue with known CCDs and pipe new ones through
        (async/onto-chan queue (sort-by :timestamp ccds) false)
        (async/pipe submissions queue)
        ;; setup complete, create our "worker" loops and return
        (log/info "starting initializer")
        (let [submission-loop (kafka.util/ccd-topic-onto-chan
                               submissions
                               control-topic
                               kafka-config)
              process-loop (initialize-ccd-loop
                            queue
                            updates-chan
                            change-data-store
                            seed-store
                            topic-store)]
          (assoc this
                 :queue queue
                 :submission-loop submission-loop
                 :process-loop process-loop
                 :producer producer)))))
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
        (when-let [p (:producer this)]
          (.close p))
        (log/info "stopped initializer")
        (dissoc this :submission-loop :process-loop :queue :producer))
      this)))

(defn new-initializer
  "Returns a new, un-started, Change Data Capture Initializer processing
  definitions posted to the specified Kafka `control-topic` in the quorum
  accessible via the given `zk-connect` ZooKeeper connection string.

  Will read and process existing definitions from the topic when
  started and process new submissions as they are posted."
  [zk-connect control-topic]
  (component/using
   (map->ChangeCaptureInitializer
    {:zk-connect zk-connect
     :control-topic control-topic})
   [:change-data-store
    :seed-store
    :topic-store]))
