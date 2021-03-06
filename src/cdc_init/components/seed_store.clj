(ns cdc-init.components.seed-store
  (:require [clojure.core.async :as async]
            [clojure.java.jdbc :as jdbc :refer [with-db-connection]]
            [clojure.string :as string]

            [com.stuartsierra.component :as component]

            [clj-time.jdbc]
            [clj-time.coerce :as time.coerce]
            [yesql.core :refer [defquery]]
            [yesql.util :refer [slurp-from-classpath]]

            [cheshire.core :as cheshire]

            [cdc-init.protocols :refer [SeedStore]]
            [cdc-init.sql.util :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Queries

(defquery count-seeds "cdc_init/sql/seeds/record_count.sql")
(def -create-seed-view! (slurp-from-classpath "cdc_init/sql/seeds/create_seed_view.sql"))
(defquery drop-seed-view! "cdc_init/sql/seeds/drop_seed_view.sql")

(defn create-seed-view!
  [{:keys [schema table alias]} {:keys [connection]}]
  (let [stmt (doto (.prepareCall connection -create-seed-view!)
               (.registerOutParameter 1 java.sql.Types/VARCHAR)
               (.setString 2 schema)
               (.setString 3 table)
               (.setString 4 alias)
               (.execute))]
    (.getString stmt 1)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(defn seed-row->dml-msg
  "Returns a row from a Change Data Capture seed view query
  re-formatted as a DML message."
  [row]
  (reduce
   (fn [rs [k v]]
     (if (.startsWith (str k) ":cdc.")
       (assoc-in rs (map keyword (rest (string/split (str k) #"\."))) v)
       (assoc-in rs [:data k] v)))
   {}
   row))

(defn dml-msg->seed-msg
  "Returns a `{:key ... :value ...}` map where the `:key` and `:value`
  are appropriate JSON strings derived from the given DML message map."
  [dml]
  {:key (cheshire/generate-string (flatten (sort-by first (vec (:id dml)))))
   :value (cheshire/generate-string dml)})

(defn seed-results-onto-chan
  "Given a JDBC `ResultSet` for a seed view, loops over every row in the result
  set, coverts it to a DML seed message, and puts the message onto the provided
  channel.

  Does what it can to avoid creating lazy sequences or otherwise holding onto
  each row/message after it has been read."
  [ch rs]
  (let [rsmeta (.getMetaData rs)
        idxs (range 1 (inc (.getColumnCount rsmeta)))
        keys (->> idxs
                  (map (fn [^Integer i] (.getColumnLabel rsmeta i)))
                  (#'jdbc/make-cols-unique)
                  (map (comp keyword string/lower-case)))
        row-values (fn []
                     (map
                      (fn [^Integer i]
                        (jdbc/result-set-read-column
                         (.getObject rs i)
                         rsmeta i))
                      idxs))]
    (while (.next rs)
      (let [row (zipmap keys (row-values))
            dml (-> row seed-row->dml-msg dml-msg->seed-msg)]
        (async/>!! ch dml)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord IFSSeedStore [database]
  component/Lifecycle
  (start [this] this)
  (stop [this] this)

  SeedStore
  (record-count [this table]
    (count-seeds (split-table-ref table)
                 {:connection database
                  :result-set-fn (fn [rs] (-> rs first :n int))}))
  (to-chan [this table]
    (.to-chan this table nil))
  (to-chan [this table table-alias]
    (let [table-ref (assoc
                     (split-table-ref table)
                     :alias table-alias)
          ch (async/chan)]
      (async/go
        (try
          (with-db-connection [db database]
            (let [seed-view (create-seed-view! table-ref db)]
              (jdbc/db-query-with-resultset
               db
               [(str "select * from " seed-view)]
               (partial seed-results-onto-chan ch))))
          (finally
            (try (drop-seed-view! table-ref {:connection database})
                 (catch Exception e nil))
            (async/close! ch))))
      ch)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn new-seed-store
  "Returns a new, un-started, IFS Seed Store.

  Requires an `:database` component that encapsulates the JDBC
  connection to an IFS database."
  []
  (component/using
   (map->IFSSeedStore {})
   [:database]))
