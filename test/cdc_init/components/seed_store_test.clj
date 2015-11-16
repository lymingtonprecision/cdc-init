(ns cdc-init.components.seed-store-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck.clojure-test :as chuck]

            [clojure.string :as string]
            [cheshire.core :as cheshire]
            [cdc-init.test-generators :refer :all]

            [cdc-init.components.seed-store :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Generator fns

(def gen-prefixed-column
  (gen/fmap
   (fn [s] (keyword (str "cdc." (string/join "." s))))
   (gen/vector (gen/not-empty gen/string-alphanumeric) 1 5)))

(def gen-id-column
  (gen/fmap
   (fn [s] (keyword (str "cdc.id." s)))
   (gen/not-empty gen/string-alphanumeric)))

(def gen-column-value
  (gen/one-of
   [(gen/return nil)
    gen/boolean
    gen/string-ascii
    gen/neg-int
    gen/pos-int]))

(def gen-seed-row
  (gen/tuple
   (gen/fmap
    (fn [[id pf]] (merge id pf))
    (gen/tuple
     (gen/not-empty (gen/map gen-id-column gen-column-value))
     (gen/map gen-prefixed-column gen-column-value)))
   (gen/not-empty (gen/map (gen/fmap keyword (gen/not-empty gen/string-ascii)) gen-column-value))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; seed-row->dml-msg

(defspec seed-row->dml-msg-conversion
  (chuck/for-all
   [[prefixed-cols non-prefixed-cols] gen-seed-row]
   (let [row (merge prefixed-cols non-prefixed-cols)
         exp (reduce
              (fn [rs [k v]]
                (assoc-in rs (map keyword (rest (string/split (str k) #"\."))) v))
              {:data non-prefixed-cols}
              prefixed-cols)]
     (is (= exp (seed-row->dml-msg row))))))

(deftest seed-row->dml-msg-example
  (let [row {:cdc.type "insert"
             :cdc.table "ifsapp.shop_ord_tab"
             :cdc.info.user_id "ifsapp"
             :cdc.info.timestamp "2015-11-13T12:30:56.467000+00:00"
             :cdc.id.order_no "123456"
             :cdc.id.release_no "*"
             :cdc.id.sequence_no "*"
             :order_no "123456"
             :release_no "*"
             :sequence_no "*"}
        exp {:id {:order_no "123456"
                  :release_no "*"
                  :sequence_no "*"}
             :type "insert"
             :table "ifsapp.shop_ord_tab"
             :data {:order_no "123456"
                    :release_no "*"
                    :sequence_no "*"}
             :info {:user_id "ifsapp"
                    :timestamp "2015-11-13T12:30:56.467000+00:00"}}]
    (is (= exp (seed-row->dml-msg row)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; dml-msg->seed-msg

(deftest dml-msg->seed-msg-example
  (let [dml (sorted-map
             :id (let [id {:order_no "123456" :release_no "7" :sequence_no "*"}]
                   (into
                    (sorted-map-by
                     (fn [k1 k2]
                       (compare [(get id k1) k1] [(get id k2) k2])))
                    id))
             :type "insert"
             :table "ifsapp.shop_ord_tab"
             :data (sorted-map
                    :order_no "123456"
                    :release_no "*"
                    :sequence_no "*"
                    :rowstate "Closed")
             :info (sorted-map
                    :user_id "ifsapp"
                    :timestamp "2015-11-13T12:30:56.467000+00:00"))
        exp {:key "[\"order_no\",\"123456\",\"release_no\",\"7\",\"sequence_no\",\"*\"]"
             :value (cheshire/generate-string dml)}]
    (is (= exp (dml-msg->seed-msg dml)))))
