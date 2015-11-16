(ns cdc-init.protocols)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defprotocol ChangeDataStore
  (queue-exists? [this queue])
  (create-queue! [this queue table])
  (clear-queue! [this queue table])
  (trigger-exists? [this table])
  (trigger-enabled? [this table])
  (create-trigger! [this table queue] [this table queue trigger])
  (enable-trigger! [this table])
  (disable-trigger! [this table]))

(defprotocol TopicStore
  (topic-exists? [this topic])
  (create-topic! [this topic])
  (clear-topic! [this topic])
  (>!topic [this topic value] [this topic key value]
    "Asynchronously posts a message with the specified key/value to
    the topic, returning a future encapsulating the send."))

(defprotocol SeedStore
  (record-count [this table])
  (to-chan [this table]
    "Returns a channel onto which each seed record will be put until
    no records remain (and the channel is closed.)"))
