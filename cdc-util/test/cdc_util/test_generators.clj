(ns cdc-util.test-generators
  (:require [clojure.test.check.generators :as gen]
            [clojure.string :as string]
            [cheshire.core :as cheshire]
            [clj-kafka.consumer.zk :as kafka.consumer]
            [clj-kafka.consumer.util :as k.c.util]
            [clj-time.core :as time]
            [cdc-util.format :as format]
            [cdc-util.schema.oracle-refs-test :as oracle-refs])
  (:import [kafka.message Message MessageAndMetadata]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(def gen-rand-ccd-status
  (gen/elements
   [:submitted
    :prepared
    :active
    :queue-created
    :topic-created
    :trigger-created]))

(def gen-rand-time
  (gen/fmap
   (fn [[u p]] (time/plus (time/now) (p u)))
   (gen/tuple gen/int
              (gen/elements
               [time/days
                time/months
                time/weeks
                time/hours
                time/minutes
                time/seconds]))))

(def gen-change-capture-def
  (gen/hash-map
   :table oracle-refs/gen-schema-ref
   :table-alias oracle-refs/gen-table-alias
   :status gen-rand-ccd-status
   :timestamp gen-rand-time
   :queue oracle-refs/gen-queue-ref
   :queue-table oracle-refs/gen-queue-ref))

(defn new-change-capture-def []
  (first (gen/sample gen-change-capture-def 1)))

(def gen-seed-value
  (gen/such-that
   #(and (some? %)
         (not (instance? Boolean %)))
   gen/simple-type-printable))

(def gen-seed
  (gen/hash-map :key gen/nat :value gen-seed-value))

(def gen-seeds
  (gen/vector gen-seed))

(defn sample-seeds
  ([] (sample-seeds (rand-int 100)))
  ([n] (gen/sample gen-seed n)))

(defn ccd->msg [ccd topic offset]
  (MessageAndMetadata.
   topic 0
   (Message. (.getBytes (cheshire/generate-string ccd) "UTF8")
             (.getBytes (:table ccd) "UTF8"))
   offset
   (k.c.util/string-decoder) #_(kafka.consumer/default-decoder)
   (format/ccd-decoder) #_(kafka.consumer/default-decoder)))
