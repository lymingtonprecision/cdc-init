(ns cdc-util.validate
  (:require [schema.core :as schema]
            [cdc-util.schema.oracle-refs :as oracle-refs]
            [cdc-util.schema :refer [CCD]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(def ccd-checker
  (schema/checker CCD))

(def ccd-alias-checker
  (schema/checker {:table-alias oracle-refs/table-alias schema/Any schema/Any}))

(defn table-name [ccd]
  (let [table (str (:table ccd))
        table-ref (when table (re-matches oracle-refs/schema-ref-regex table))]
    (when table-ref
      (->> table-ref last (re-matches #"^\"?([^\"]+)\"?$") last))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn requires-table-alias?
  "Returns truthy if the given Change Capture Definition requires but
  is missing a table alias entry otherwise returns false if either no
  alias is required or one has already been provided."
  [ccd]
  (and (> (count (table-name ccd)) 22)
       (nil? (:table-alias ccd))))

(defn check-ccd
  "Returns `nil` if the provided Change Capture Definition record is
  valid otherwise returns a map describing the 'bad' parts."
  [ccd]
  (let [err (ccd-checker ccd)
        alias-err (when (and (requires-table-alias? ccd) (nil? (:table-alias err)))
                    (ccd-alias-checker ccd))]
    (when (or err alias-err)
      (merge err alias-err))))

(defn validate-ccd
  "Returns the provided Change Capture Definition record if it
  conforms to the expected schema otherwise returns `nil`."
  [ccd]
  (when-not (check-ccd ccd)
    ccd))
