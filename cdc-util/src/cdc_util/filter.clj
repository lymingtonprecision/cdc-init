(ns cdc-util.filter
  (:require [cdc-util.format :refer [msg->ccd]])
  (:import [kafka.message MessageAndMetadata]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants

(def non-initializable-statuses
  #{:active :error})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(defn topic->ccds-and-max-offset
  "Given a reduceable collection of `MessageAndMetadata` records
  returns a map of `{:max-offset 0 :ccds [...ccd...]}` where
  `:max-offset` is the maximum message offset read and `:ccds` is a
  collection of Change Data Capture Definitions derived from the
  message bodies.

  Only the latest (in terms of its sequence in the given collection)
  Change Data Capture Definition is returned per message key.

  **Note:** the provided `MessageAndMetadata` instances **must**
  be using the `cdc-init.format/ccd-decoder` to return the message
  bodies as Change Data Capture Definition records."
  [reduceable-topic]
  (update
   (reduce
    (fn [{:keys [max-offset ccds]} ^MessageAndMetadata m]
      {:max-offset (max max-offset (.offset m))
       :ccds (assoc ccds (.key m) (.message m))})
    {:max-offset -1 :ccds {}}
    reduceable-topic)
   :ccds vals))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn msgs->ccds-with-status
  "Returns a transducer that converts Kafka `MessageAndMetadata`
  instances to Change Capture Definition records and returns only
  those with the specified status.

  **Note:** the provided `MessageAndMetadata` instances **must**
  be using the `cdc-init.format/ccd-decoder` to return the message
  bodies as Change Data Capture Definition records."
  [status]
  (comp (map #(.message %))
        (filter #(= status (:status %)))))

(defn topic->ccds-to-initialize
  "Given a reduceable collection of `MessageAndMetadata` records
  comprising Change Capture Definitions posted to a Kafka topic
  returns a tuple of `[ccds-to-initialize max-offset]` where:

  * `ccds-to-initialize` is a collection of Change Capture Definitions
    read from the topic that require initialization.
  * `max-offset` is the highest message offset read.

  **Note:** the provided `MessageAndMetadata` instances **must**
  be using the `cdc-init.format/ccd-decoder` to return the message
  bodies as Change Data Capture Definition records."
  [reduceable-topic]
  (let [{:keys [max-offset ccds]} (topic->ccds-and-max-offset reduceable-topic)]
    [(doall (remove #(contains? non-initializable-statuses (:status %)) ccds))
     max-offset]))
