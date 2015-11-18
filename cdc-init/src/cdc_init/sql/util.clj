(ns cdc-init.sql.util
  (:require [clojure.java.jdbc :as jdbc :refer [with-db-connection]]
            [clojure.string :as string]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Regexes

(def ^:private ora-schema-obj-regex-str
 "(\"[\\s[^\"] ]+\"|[a-z][a-z0-9_$#]*)")

(def ^:private ora-schema-obj-regex
  (re-pattern (str "(?i)" ora-schema-obj-regex-str)))

(def ^:private ora-schema-ref-regex
  (re-pattern (str "(?i)" ora-schema-obj-regex-str "\\." ora-schema-obj-regex-str)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn in-own-schema?
  "Returns true if the given database object reference, `ref`, relates
  to an object in the database users schema.

  Assumes any non-schema qualified references belong to the user."
  [db ref]
  (if-let [s (second (re-matches ora-schema-ref-regex ref))]
    (= (string/replace (string/lower-case s) #"\"" "")
       (-> db :options :username string/lower-case))
    true))

(defn strip-schema
  "Removes the schema from the given database object reference."
  [ref]
  (or (last (re-matches ora-schema-ref-regex ref)) ref))

(defn mq-table
  "Returns the underlying message queue table reference for the given
  queue table reference."
  [t]
  (let [st (rest (or (re-matches ora-schema-ref-regex t)
                     (re-matches ora-schema-obj-regex t)))
        mq (some-> (or (second st) (first st))
                   (string/replace #"^\"?" #(str %1 "mq_")))]
    (if (and mq (= (count st) 2))
      (str (first st) "." mq)
      mq)))

(defn split-table-ref
  "Returns a map of `{:schema ... :table ...}` from the given table
  reference string."
  [ref]
  (let [[_ s t] (re-matches ora-schema-ref-regex ref)]
    (when-not s
      (throw (Exception. (str "schema must be provided for table reference "
                              ref))))
    {:schema s
     :table t}))

(defn call-plsql-test!
  [db plsql & params]
  (with-db-connection [db db]
    (let [stmt (doto (.prepareCall (:connection db) plsql)
                 (.registerOutParameter 1 java.sql.Types/INTEGER))]
      (doseq [[i p] (map-indexed (fn [i p] [(+ i 2) p]) params)]
        (.setString stmt i p))
      (.execute stmt)
      (pos? (.getInt stmt 1)))))
