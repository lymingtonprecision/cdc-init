(ns cdc-util.schema
  (:require [schema.core :as schema :refer [defschema]]
            [cdc-util.schema.oracle-refs :as oracle-refs]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Constants

(def ccd-states
  [:submitted
   :trigger-created
   :queue-created
   :topic-created
   :prepared
   :seeding
   :active
   :error])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Predicates

(defschema int>=zero
  (schema/pred #(and (integer? %) (>= % 0))
               "integer >= 0"))

(defschema int>zero
  (schema/pred #(and (integer? %) (pos? %))
               "integer > 0"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Private, interstitial, schema

(defschema -ccd
  {:table oracle-refs/db-schema-ref
   (schema/optional-key :table-alias) oracle-refs/table-alias
   :queue oracle-refs/queue-ref
   :queue-table oracle-refs/queue-ref
   :status (apply schema/enum ccd-states)
   :timestamp org.joda.time.DateTime})

(defschema -ccd-error
  {:status (schema/eq :error)
   :error {:message schema/Str
           schema/Keyword schema/Any}})

(defschema -ccd-seeding
  {:status (schema/eq :seeding)
   :progress (schema/pair int>=zero :progress int>zero :total)})

(defschema -ccd-with-error
  (merge -ccd -ccd-error))

(defschema -ccd-being-seeded
  (merge -ccd -ccd-seeding))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defschema CCD
  (schema/conditional
   #(= :error (:status %)) -ccd-with-error
   #(= :seeding (:status %)) -ccd-being-seeded
   :else -ccd))
