(ns cdc-util.validate)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Validity helpers

(defn- valid-reference? [r]
  (and (instance? String r)
       (not (clojure.string/blank? r))))

(defn- every-reference-valid? [ccd]
  (every? valid-reference?
          (map #(get ccd %) [:table :queue :queue-table :trigger])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn validate-ccd
  "Returns the provided Change Capture Definition record if it
  conforms to the expected schema otherwise returns `nil`."
  [ccd]
  (some-> ccd
          (#(when (every-reference-valid? %) %))
          (#(when (instance? org.joda.time.DateTime (:timestamp %)) %))))
