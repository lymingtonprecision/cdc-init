(ns cdc-init.filter-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck.clojure-test :as chuck]

            [cheshire.core :as cheshire]
            [clj-kafka.consumer.zk :as kafka.consumer]
            [clj-time.format :as time.format]

            [cdc-init.test-generators :refer :all]
            [cdc-init.filter :refer :all])
  (:import [kafka.message Message MessageAndMetadata]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; msgs->submitted-ccds

(defspec submitted-filters-out-submitted-defs-from-message-collections
  (chuck/for-all
   [topic gen/string-ascii
    ccds (gen/vector (gen/tuple gen/nat gen-change-capture-def))]
   (let [msgs (map (fn [[offset ccd]] (ccd->msg ccd topic offset)) ccds)
         exp (filter #(= :submitted (:status %)) (map second ccds))]
     (is (= exp (transduce msgs->submitted-ccds conj [] msgs))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; topic->ccds-to-initialize

(defspec ccds-to-init-returns-ccds-and-max-offset
  (chuck/for-all
   [topic gen/string-ascii
    ccds (gen/vector (gen/tuple gen/nat gen-change-capture-def))]
   (let [msgs (map (fn [[offset ccd]] (ccd->msg ccd topic offset)) ccds)
         exp [(remove #(contains? #{:active :error} (:status %))
                      (vals
                       (reduce
                        (fn [rs ccd] (assoc rs (:table ccd) ccd))
                        {}
                        (map second ccds))))
              (if (empty? ccds)
                -1
                (apply max (map first ccds)))]]
     (is (= exp (topic->ccds-to-initialize msgs))))))
