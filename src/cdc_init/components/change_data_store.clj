(ns cdc-init.components.change-data-store
  (:require [clojure.string :as string]
            [com.stuartsierra.component :as component]
            [yesql.core :refer [defquery]]
            [yesql.util :refer [slurp-from-classpath]]
            [cdc-init.protocols :refer [ChangeDataStore]]
            [cdc-init.sql.util :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Queries

(def -queue-exists? (slurp-from-classpath "cdc_init/sql/queue/queue_exists.sql"))
(defquery -create-queue! "cdc_init/sql/queue/create_queue.sql")
(defquery -clear-queue! "cdc_init/sql/queue/clear_queue.sql")

(def -trigger-exists? (slurp-from-classpath "cdc_init/sql/trigger/trigger_exists.sql"))
(def -trigger-enabled? (slurp-from-classpath "cdc_init/sql/trigger/trigger_enabled.sql"))
(defquery -create-trigger! "cdc_init/sql/trigger/create_trigger.sql")
(defquery -enable-trigger! "cdc_init/sql/trigger/enable_trigger.sql")
(defquery -disable-trigger! "cdc_init/sql/trigger/disable_trigger.sql")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord IFSChangeDataStore [database]
  component/Lifecycle
  (start [this] this)
  (stop [this] this)

  ChangeDataStore
  (queue-exists? [this queue]
    (call-plsql-test! database -queue-exists? queue))

  (create-queue! [this queue table]
    (when-not (every? (partial in-own-schema? database) [queue table])
      (throw (Exception. (str "can only create queues in own schema "
                              (string/join ", " [queue table])
                              " not owned by "
                              (-> database :options :username)))))
    (-create-queue! {:table (strip-schema table)
                     :name (strip-schema queue)}
                    {:connection database}))

  (clear-queue! [this queue table]
    (-clear-queue! {:queue (strip-schema queue)
                    :table (mq-table table)}
                   {:connection database}))

  (trigger-exists? [this table]
    (let [{:keys [schema table]} (split-table-ref table)]
      (call-plsql-test! database -trigger-exists? schema table)))
  (trigger-enabled? [this table]
    (let [{:keys [schema table]} (split-table-ref table)]
      (call-plsql-test! database -trigger-enabled? schema table)))

  (create-trigger! [this table queue]
    (.create-trigger! database table queue nil))
  (create-trigger! [this table queue trigger]
    (-create-trigger! (merge
                       (split-table-ref table)
                       {:queue queue
                        :trigger trigger})
                      {:connection database}))

  (enable-trigger! [this table]
    (-enable-trigger! (split-table-ref table) {:connection database}))
  (disable-trigger! [this table]
    (-disable-trigger! (split-table-ref table) {:connection database})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn new-change-data-store
  "Returns a new, un-started, IFS Change Data Store.

  Requires an `:database` component that encapsulates the JDBC
  connection to an IFS database."
  []
  (component/using
   (map->IFSChangeDataStore {})
   [:database]))
