(ns cdc-util.validate
  (:require [schema.core :as schema]
            [cdc-util.schema :refer [CCD]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn validate-ccd
  "Returns the provided Change Capture Definition record if it
  conforms to the expected schema otherwise returns `nil`."
  [ccd]
  (when-not (or (schema/check CCD ccd)
                (requires-table-alias? ccd))
    ccd))
