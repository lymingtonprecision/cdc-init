(ns cdc-util.components.database
  "A component to manage (pooled) database connections using HikariCP.

  Defaults to using the Oracle Thin driver for connections with a
  min/max pool size of 2/20 connections."
  (:require [com.stuartsierra.component :as component]
            [hikari-cp.core :as hk]
            [cdc-util.env :refer [env->config]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defaults

(def default-options
  (merge
   hk/default-datasource-options
   {:auto-commit true
    :read-only false
    :minimum-idle 2
    :maximum-pool-size 20
    :adapter "oracle"
    :driver-type "thin"
    :port-number 1521}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ENV key mappings

(def env-keys->option-names
  [[:db-name :database-name]
   [:db-server :server-name]
   [:db-user :username]
   [:db-password :password]])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(defrecord Database [env]
  component/Lifecycle
  (start [this]
    (let [o (merge default-options
                   (env->config env env-keys->option-names))
          ds (hk/make-datasource o)]
      (assoc this :options o :datasource ds)))
  (stop [this]
    (if-let [ds (:datasource this)]
      (hk/close-datasource ds))
    (dissoc this :datasource)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn new-database
  "Returns a new, un-started, database component.

  Requires an `:env` component containing the key/value mappings for:

  * `:db-server`
  * `:db-name`
  * `:db-user`
  * `:db-password`"
  []
  (component/using
   (map->Database {})
   [:env]))
