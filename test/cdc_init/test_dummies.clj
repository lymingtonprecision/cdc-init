(ns cdc-init.test-dummies
  (:require [clojure.core.async :as async]
            [cdc-init.protocols :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; error generation

(defprotocol ErrorGenerator
  (-error-on! [this event f])
  (get-error [this event])
  (reset-errors! [this] [this event]))

(defn ^ErrorGenerator error-on!
  "Registers `f` as the error generator for the specified `event`.

  Whenever `event` occurs `f` will be called (with arguments relevant
  to the event) and thereby given an opportunity to throw an exception
  and interrupt processing. (If `f` does _not_ throw an exception then
  processing will continue as normal.)

  If `f` is not specified it defaults to a fn that takes any number of
  arguments and throws an `Exception` when called."
  ([this event]
   (error-on! this event (fn [& args] (throw (Exception. "generated test error")))))
  ([this event f]
   (-error-on! this event f)))

(defn ^ErrorGenerator error-on?!
  "Calls the fn registered as the error generator for `event` with the
  provided `args`, if one is registered."
  [this event & args]
  (when-let [e (get-error this event)]
    (apply e args)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; dummy implementations

(defrecord DummyDatabase [queues triggers errors]
  ErrorGenerator
  (-error-on! [this event f] (swap! errors assoc event f))
  (get-error [this event] (get @errors event))
  (reset-errors! [this] (reset! errors {}))
  (reset-errors! [this event] (swap! errors dissoc event))
  ChangeDataStore
  (queue-exists? [this queue]
    (contains? @queues queue))
  (create-queue! [this queue table]
    (when-not (queue-exists? this queue)
      (error-on?! this :create-queue queue @queues)
      (swap! queues assoc queue [])))
  (clear-queue! [this queue table]
    (when (queue-exists? this queue)
      (swap! queues assoc queue [])))
  (trigger-exists? [this table]
    (contains? @triggers table))
  (trigger-enabled? [this table]
    (get @triggers table))
  (create-trigger! [this table queue]
    (create-trigger! this table queue nil))
  (create-trigger! [this table queue table-alias]
    (when-not (trigger-exists? this table)
      (error-on?! this :create-trigger table @triggers)
      (swap! triggers assoc table false)))
  (enable-trigger! [this table]
    (when (trigger-exists? this table)
      (swap! triggers assoc table true)))
  (disable-trigger! [this table]
    (when (trigger-exists? this table)
      (swap! triggers assoc table false))))

(defn new-dummy-database
  "Creates and returns a new dummy database, storing the queue and
  triggers in atoms wrapping maps.

  Errors can be generated from the returned database on the following
  events, with the specified error fn signatures:

  * `:create-queue`, `(fn [queue-name existing-queues])`
  * `:create-trigger`, `(fn [table-name existing-triggers])`"
  []
  (map->DummyDatabase {:queues (atom {}) :triggers (atom {}) :errors (atom {})}))

(defrecord DummySeedStore [seeds errors]
  ErrorGenerator
  (-error-on! [this event f] (swap! errors assoc event f))
  (get-error [this event] (get @errors event))
  (reset-errors! [this] (reset! errors {}))
  (reset-errors! [this event] (swap! errors dissoc event))
  SeedStore
  (record-count [this table] (count (get @seeds table)))
  (to-chan [this table]
    (.to-chan this table nil))
  (to-chan [this table table-alias]
    (error-on?! this :to-chan table @seeds)
    (async/to-chan (get @seeds table))))

(defn new-dummy-seeds
  "Creates and returns a new dummy `SeedStore`, storing the seed
  records in an atom wrapping a map.

  Errors can be generated from the returned store on the following
  events, with the specified error fn signatures:

  * `:to-chan`, `(fn [table-name existing-seeds])`"
  []
  (map->DummySeedStore {:seeds (atom {}) :errors (atom {})}))

(defrecord DummyKafka [topics errors]
  ErrorGenerator
  (-error-on! [this event f] (swap! errors assoc event f))
  (get-error [this event] (get @errors event))
  (reset-errors! [this] (reset! errors {}))
  (reset-errors! [this event] (swap! errors dissoc event))
  TopicStore
  (topic-exists? [this topic]
    (contains? @topics topic))
  (create-topic! [this topic]
    (when-not (topic-exists? this topic)
      (error-on?! this :create-topic topic @topics)
      (swap! topics assoc topic [])))
  (clear-topic! [this topic]
    (when (topic-exists? this topic)
      (swap! topics assoc topic [])))
  (>!topic [this topic value]
    (>!topic this topic nil value))
  (>!topic [this topic key value]
    (when-not (topic-exists? this topic)
      (throw (Exception. (str "invalid topic " topic))))
    (error-on?! this :send topic key value (get @topics topic))
    (future (swap! topics update topic (fn [t] (conj t {:key key :value value}))))))

(defn new-dummy-kafka
  "Creates and returns a new dummy `TopicStore`, storing the topics in
  an atom wrapping a map.

  Errors can be generated from the returned store on the following
  events, with the specified error fn signatures:

  * `:create-topic`, `(fn [topic-name existing-topics])`
  * `:send`, `(fn [topic-name key value topic-contents])`"
  []
  (map->DummyKafka {:topics (atom {}) :errors (atom {})}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(def ^:dynamic *dummy-database*)
(def ^:dynamic *dummy-kafka*)
(def ^:dynamic *dummy-seeds*)

(defn reset-dummies!
  ([]
   (alter-var-root #'*dummy-database* (fn [_] (new-dummy-database)))
   (alter-var-root #'*dummy-kafka* (fn [_] (new-dummy-kafka)))
   (alter-var-root #'*dummy-seeds* (fn [_] (new-dummy-seeds))))
  ([t]
   (reset-dummies!)
   (t)))
