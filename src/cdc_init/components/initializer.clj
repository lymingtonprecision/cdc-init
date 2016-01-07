(ns cdc-init.components.initializer
  "A component to process Change Data Capture initialization
  requests."
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]

            [cdc-util.async :refer [pipe-ret-last go-till-closed noop-transducer]]
            [cdc-util.filter :as filter]
            [cdc-util.kafka :refer :all]
            [cdc-util.validate :refer [check-ccd]]

            [cdc-init.core :refer :all]))

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
  (remove
   #(contains? non-initializable-statuses (:status %))
   (topic->last-known-ccd-states kafka-config topic)))

(defn updates-chan
  "Returns a channel that will post any received Change Capture
  Definition updates to the specified topic via the given Kafka
  producer."
  [kafka-producer topic]
  (async/chan 1 (noop-transducer (partial send-ccd kafka-producer topic))))

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
    [control-topic kafka change-data-store seed-store topic-store]
  component/Lifecycle
  (start [this]
    (if (:process-loop this)
      this
      (do
        (log/info "starting initializer")
        (when-not (topic-exists? (:config kafka) control-topic)
          (create-control-topic! (:config kafka) control-topic))
        (let [ccds (ccds-to-initialize (:config kafka) control-topic)
              _ (log/info "found" (count ccds) "CCDs waiting initializtion")
              ;; create a work queue
              ;; ... and seed it with the definitions awaiting initialization
              queue (async/chan 100)
              _ (async/onto-chan queue (sort-by :timestamp ccds) false)
              ;; create a channel to filter out new submissions from updates
              ;; ... and pipe those new submissions through to the work queue
              submissions (async/chan
                           1
                           (filter/msgs->ccds-with-status :submitted))
              _ (async/pipe submissions queue)
              ;; synchronously post progress updates back to kafka
              updates (updates-chan (:producer kafka) control-topic)]
          ;; setup complete, create our "worker" loops and return
          (let [submission-loop (ccd-topic-onto-chan
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
