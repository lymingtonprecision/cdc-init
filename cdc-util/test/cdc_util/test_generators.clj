(ns cdc-util.test-generators
  (:require [clojure.test.check.generators :as gen]
            [clojure.string :as string]
            [cheshire.core :as cheshire]
            [clj-kafka.consumer.zk :as kafka.consumer]
            [clj-kafka.consumer.util :as k.c.util]
            [clj-time.core :as time]
            [cdc-util.format :as format])
  (:import [kafka.message Message MessageAndMetadata]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(def gen-rand-ccd-status
  (gen/elements
   [:submitted
    :prepared
    :active
    :error
    :queue-created
    :topic-created
    :trigger-created]))

(def gen-non-empty-string
  (gen/such-that
   #(not (string/blank? %))
   (gen/not-empty gen/string-ascii)))

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
   :table gen-non-empty-string
   :status gen-rand-ccd-status
   :timestamp gen-rand-time
   :trigger gen-non-empty-string
   :queue gen-non-empty-string
   :queue-table gen-non-empty-string))

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
   (Message. (.getBytes (cheshire/generate-string ccd))
             (.getBytes (:table ccd)))
   offset
   (k.c.util/string-decoder) #_(kafka.consumer/default-decoder)
   (format/ccd-decoder) #_(kafka.consumer/default-decoder)))
