(ns cdc-util.env-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck.clojure-test :as chuck]
            [cdc-util.env :refer :all]))

(defspec env->config-selects-given-keys
  50
  (chuck/for-all
   [env (gen/map gen/keyword gen/simple-type-printable)]
   (let [keys (->> env keys vec shuffle (take (rand-int (count env))))]
     (is (= (select-keys env keys) (env->config env keys))))))

(deftest env->config-uses-replacements-when-given
  (let [env {:db-host "db.example.com"
             :db-name "app-db"
             :db-user "admin"
             :db-pass "secret"
             :source-table "orders"
             :destination-table "order_summary"}
        keys [:source-table
              :destination-table
              [:db-host :server]
              [:db-name :database]
              [:db-user :username]
              [:db-pass :password]]
        exp (merge
             {:server (:db-host env)
              :database (:db-name env)
              :username (:db-user env)
              :password (:db-pass env)}
             (select-keys env [:source-table :destination-table]))]
    (is (= exp (env->config env keys)))))
