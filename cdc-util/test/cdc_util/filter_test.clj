(ns cdc-util.filter-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck.clojure-test :as chuck]

            [cheshire.core :as cheshire]
            [clj-kafka.consumer.zk :as kafka.consumer]
            [clj-time.format :as time.format]

            [cdc-util.test-generators :refer :all]
            [cdc-util.filter :refer :all])
  (:import [kafka.message Message MessageAndMetadata]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; msgs->ccds-with-status

(defspec msgs->ccds-with-status-filters-message-collections
  (chuck/for-all
   [topic gen/string-ascii
    ccds (gen/vector (gen/tuple gen/nat gen-change-capture-def))
    status gen-rand-ccd-status]
   (let [msgs (map (fn [[offset ccd]] (ccd->msg ccd topic offset)) ccds)
         exp (filter #(= status (:status %)) (map second ccds))]
     (is (= exp (transduce (msgs->ccds-with-status status) conj [] msgs))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; topic->ccds-and-max-offset

(defspec ccds-and-max-offset
  (chuck/for-all
   [topic gen/string-ascii
    ccds (gen/vector (gen/tuple gen/nat gen-change-capture-def))]
   (let [msgs (map (fn [[offset ccd]] (ccd->msg ccd topic offset)) ccds)
         exp [(vals
               (reduce
                (fn [rs ccd] (assoc rs (:table ccd) ccd))
                {}
                (map second ccds)))
              (if (empty? ccds)
                -1
                (apply max (map first ccds)))]]
     (is (= exp (topic->ccds-and-max-offset msgs))))))
