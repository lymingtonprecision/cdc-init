(ns cdc-util.filter
  (:import [kafka.message MessageAndMetadata]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn topic->ccds-and-max-offset
  "Given a reduceable collection of `MessageAndMetadata` records
  returns a tuple of `[ccds max-offset]` where `max-offset` is the
  maximum message offset read and `ccds` is a collection of Change
  Data Capture Definitions derived from the message bodies.

  Only the latest (in terms of its sequence in the given collection)
  Change Data Capture Definition is returned per message key.

  **Note:** the provided `MessageAndMetadata` instances **must**
  be using the `cdc-init.format/ccd-decoder` to return the message
  bodies as Change Data Capture Definition records."
  [reduceable-topic]
  (update
   (reduce
    (fn [[ccds max-offset] ^MessageAndMetadata m]
      [(assoc ccds (.key m) (.message m))
       (max max-offset (.offset m))])
    [{} -1]
    reduceable-topic)
   0 (comp doall vals)))

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
