(ns cdc-util.schema.oracle-refs
  (:require [schema.core :as schema :refer [defschema]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utility fns

(defn schema-obj-regex-str
  "Returns a regular expression string for matching an Oracle object
  reference of up to the specified maximum length (defaulting to 30
  characters, the maximum for most Oracle database objects.)"
  ([] (schema-obj-regex-str 30))
  ([max-len]
   (str "(\"[\\s[^\"] ]{1," max-len "}\"|[a-z][a-z0-9_$#]{0," (dec max-len) "})")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Regexps

(def schema-obj-regex
  (re-pattern (str "(?i)" (schema-obj-regex-str))))

(def schema-ref-regex
  (re-pattern (str "(?i)" (schema-obj-regex-str) "\\." (schema-obj-regex-str))))

(def queue-ref-regex
  (re-pattern (str "(?i)" (schema-obj-regex-str) "\\." (schema-obj-regex-str 24))))

(def table-alias-regex
  (re-pattern (str "(?i)" (schema-obj-regex-str 22))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defschema db-schema-ref
  (schema/pred #(re-matches schema-ref-regex %)
               "DB reference (schema.obj, each <=30 chars)"))

(defschema queue-ref
  (schema/pred #(re-matches queue-ref-regex %)
               "Queue reference (schema.queue, queue <=24 chars)"))

(defschema table-alias
  (schema/pred #(re-matches table-alias-regex %)
               "Oracle object name <=22 chars"))
