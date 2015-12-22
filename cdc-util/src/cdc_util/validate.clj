(ns cdc-util.validate
  (:require [schema.core :as schema]
            [cdc-util.schema.oracle-refs :as oracle-refs]
            [cdc-util.schema :refer [CCD]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

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

(defn validate-ccd
  "Returns the provided Change Capture Definition record if it
  conforms to the expected schema otherwise returns `nil`."
  [ccd]
  (when-not (or (schema/check CCD ccd)
                (requires-table-alias? ccd))
    ccd))
