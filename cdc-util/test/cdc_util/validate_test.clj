(ns cdc-util.validate-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck.clojure-test :as chuck]

            [clojure.core.async :as async]
            [clojure.string :as string]

            [cdc-util.test-generators :refer :all]
            [cdc-util.validate :refer :all])
  (:import [java.sql Time Timestamp]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; validate-ccd

(defspec ccd-references-must-be-non-empty-strings
  (chuck/for-all
   [[valid-ccd invalid-ccd]
    (gen/fmap
     (fn [[ccd f v]]
       [ccd
        (if (= v :absent)
          (dissoc ccd f)
          (assoc ccd f v))])
     (gen/tuple
      gen-change-capture-def
      (gen/elements [:table :queue :queue-table :trigger])
      (gen/one-of
       [(gen/elements [nil :absent])
        (gen/such-that #(or (not (instance? String %)) (string/blank? %))
                       gen/any-printable)])))]
   (is (= valid-ccd (validate-ccd valid-ccd)))
   (is (nil? (validate-ccd invalid-ccd)))))

(defspec ccd-timestamps-must-be-joda-datetimes
  (chuck/for-all
   [[valid-ccd invalid-ccd]
    (gen/fmap
     (fn [[ccd u]]
       [ccd
        (if (= u :absent)
          (dissoc ccd :timestamp)
          (assoc ccd :timestamp u))])
     (gen/tuple
      gen-change-capture-def
      (gen/one-of
       [(gen/return nil)
        (gen/return :absent)
        gen/any-printable
        (gen/return (java.util.Date.))
        (gen/return (java.sql.Time. (.getTime (java.util.Date.))))
        (gen/return (java.sql.Timestamp. (.getTime (java.util.Date.))))])))]
   (is (= valid-ccd (validate-ccd valid-ccd)))
   (is (nil? (validate-ccd invalid-ccd)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; noop-transducer

(deftest noop-transducer-literally-does-nothing
  (is (empty? (into [] (noop-transducer) (range 1 100)))))

(deftest noop-transducer-calls-fn-with-input
  (let [vals (gen/sample gen/any-printable 100)
        inputs (atom [])]
    (is (empty? (into [] (noop-transducer (fn [v] (swap! inputs conj v))) vals)))
    (is (= vals @inputs))))

(deftest noop-transducer-works-with-channels
  (let [inputs [(range 1 100) (range 42 222)]
        vals (atom [])
        ch (async/chan 1 (noop-transducer (fn [v] (swap! vals conj v))))
        l (doall
           (map
            (fn [vs]
              (async/go-loop [vs vs]
                (async/>! ch (first vs))
                (when-let [vs (seq (rest vs))]
                  (recur vs))))
            inputs))]
    (loop [chs l]
      (let [[_ ch] (async/alts!! chs)]
        (when (> (count chs) 1)
          (recur (remove #(= % ch) chs)))))
    (is (= (-> inputs flatten sort) (sort @vals)))))
