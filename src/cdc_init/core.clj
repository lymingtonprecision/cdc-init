(ns cdc-init.core
  (:require [clojure.core.async :as async]
            [clj-time.core :as time]
            [cdc-init.protocols :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(defn update-status
  "Returns the Change Capture Definition, `ccd`, with its status
  replaced by `new-status`, an updated `:timestamp`, and the optional
  additional attributes `new-attrs` merged into it."
  {:no-doc true}
  ([ccd new-status]
   (update-status ccd new-status nil))
  ([ccd new-status new-attrs]
   (merge ccd
          new-attrs
          {:status new-status
           :timestamp (time/now)})))

(defmacro >!status-update
  "Puts (via `clojure.core.async/>!`) the Change Capture Definition,
  `ccd`, with its status updated to `new-status`, onto channel `c`."
  {:no-doc true}
  ([c ccd new-status]
   `(>!status-update ~c ~ccd ~new-status nil))
  ([c ccd new-status new-attrs]
   `(async/>! ~c (update-status ~ccd ~new-status ~new-attrs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn prepare
  "Prepares the database and topic store to start capturing change
  data events per the provided change capture definition (`ccd`.)

  The change capture definition, `ccd`, must specify the:

  * `:table`
    The database table from which we are capturing changes.
  * `:queue`
    The name of the queue to which the change data will be
    recorded, also serves as the name of the topic to which the
    captures will be published.
  * `:queue-table`
    The name of the queue table used as the backing store for the
    queue.

  If the specified `:table` name is longer than 22 characters then a
  `:table-alias` must also be specified to use in place of the table
  name when creating the associated objects (e.g. if your table is
  \"myschema.technical_object_reference_tab\" then an alias will be
  required and a suitable one might be \"tech_obj_ref\".)

  Any non-existing entities will be created and any that exist will be
  cleared of existing data.

  `db` **must** satisfy the `ChangeDataStore` protocol as must `ts`
  the `TopicStore` protocol.

  Returns a channel onto which the intermediate and final states of
  `ccd` are put. Possible intermediate states have a status of:

  * `:queue-created`
  * `:trigger-created`
  * `:topic-created`

  … and serve as a log entry for that point in the change capture
  preparation.

  The final state will either have a `:status` of `:prepared`, and an
  updated `:timestamp`, if preparation was successful or a `:status`
  of `:error` if an exception was encountered—with the captured
  exception stored under the `:error` key.

  The returned channel is closed when preparation has finished."
  [{:keys [table queue queue-table table-alias] :as ccd} db ts]
  {:pre [(satisfies? ChangeDataStore db)
         (satisfies? TopicStore ts)]}
  (let [progress (async/chan 4)]
    (async/go
      (try
        (if (trigger-exists? db table)
          (disable-trigger! db table)
          (do (create-trigger! db table queue table-alias)
              (>!status-update progress ccd :trigger-created)))
        (if (queue-exists? db queue)
          (clear-queue! db queue queue-table)
          (do (create-queue! db queue queue-table)
              (>!status-update progress ccd :queue-created)))
        (if (topic-exists? ts queue)
          (clear-topic! ts queue)
          (do (create-topic! ts queue)
              (>!status-update progress ccd :topic-created)))
        (>!status-update progress ccd :prepared)
        (catch Exception e
          (>!status-update progress ccd :error {:error e}))
        (finally
          (async/close! progress))))
    progress))

(defn initialize
  "Initializes the Change Capture topic with its seed records and
  enables the Change Data Capture trigger.

  The change capture definition, `ccd`, must specify the:

  * `:table`
    The database table from which we are capturing changes.
  * `:queue`
    The name of the queue/topic to which the change data will be
    recorded.

  If the specified `:table` name is longer than 22 characters then a
  `:table-alias` must also be specified to use in place of the table
  name when creating the associated objects (e.g. if your table is
  \"myschema.technical_object_reference_tab\" then an alias will be
  required and a suitable one might be \"tech_obj_ref\".)

  `ts` **must** satisfy the `TopicStore` protocol and be able to
  record new messages on the topic specified by `:queue`.

  `ss` **must** satisfy the `SeedStore` protocol and _should_ return
  the records with which to seed the Change Capture topic specified by
  `:queue` when asked.

  The seed records returned by the seed store can be any truthy
  value. If it is a map containing `:key` and `:value` entries then
  the value of those entries will be used as the key and value sent to
  the topic store, otherwise it will be sent as a non-keyed value.

  `db` **must** satisfy the `ChangeDataStore` protocol and be capable
  of enabling the trigger associated with the table.

  Returns a channel onto which the intermediate and final states of
  `ccd` during initialization are put. There is only one intermediate
  state:

      {... :status :seeding, :progress [seeded total] ...}

  That is, the change capture definition with a `:status` of
  `:seeding` and a `:progress` key specifying the current/total number
  of records seeded so far.

  The final state will have a `:status` of `:active`, and an
  updated `:timestamp`.

  Any errors during initialization will result in a final state with a
  `:status` of `:error` and the captured exception stored under the
  `:error` key.

  The returned channel is closed when initialization has finished."
  [{:keys [table queue table-alias] :as ccd} ts ss db]
  {:pre [(satisfies? TopicStore ts)
         (satisfies? SeedStore ss)
         (satisfies? ChangeDataStore db)]}
  (let [progress (async/chan (async/sliding-buffer 1))]
    (async/go
      (try
        (enable-trigger! db table)
        (let [total (record-count ss table)
              seeds (to-chan ss table table-alias)
              two-pcnt (* total 0.02)
              last-report (atom nil)]
          (when (pos? total)
            (loop [c 0]
              (when (or (nil? @last-report) (>= (- c @last-report) two-pcnt))
                (>!status-update progress ccd :seeding {:progress [c total]})
                (reset! last-report c))
              (when-let [kv (async/<! seeds)]
                (if (and (map? kv) (:key kv) (:value kv))
                  @(>!topic ts queue (:key kv) (:value kv))
                  @(>!topic ts queue kv))
                (recur (inc c))))))
        (>!status-update progress ccd :active)
        (catch Exception e
          (try (disable-trigger! db table) (catch Exception e nil))
          (>!status-update progress ccd :error {:error e}))
        (finally
          (async/close! progress))))
    progress))
