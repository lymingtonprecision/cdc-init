(ns cdc-util.validate-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck.clojure-test :as chuck]

            [clojure.core.async :as async]
            [clojure.string :as string]

            [cdc-util.schema.oracle-refs-test :as oracle-refs]
            [cdc-util.test-generators :refer :all]
            [cdc-util.validate :refer :all])
  (:import [java.sql Time Timestamp]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; requires-table-alias?

(deftest requires-table-alias-handles-nil
  (is (not (requires-table-alias? nil))))

(deftest table-alias-boundaries
  (let [max-valid (str "a." (string/join (repeat 22 "a")))]
    (is (not (requires-table-alias? {:table max-valid})))
    (is (requires-table-alias? {:table (str max-valid "a")}))))

(defspec invalid-refs-dont-need-table-aliases
  (chuck/for-all
   [ref oracle-refs/gen-invalid-schema-ref]
   (is (not (requires-table-alias? {:table ref})))))

(defspec valid-refs-need-table-aliases-when-over-22-chars
  (chuck/for-all
   [schema (oracle-refs/gen-ref)
    table (oracle-refs/gen-ref)]
   (let [ref (str schema "." table)
         raw-table (last (re-matches #"^\"?([^\"]+)\"?$" table))]
     (if (> (count raw-table) 22)
       (is (requires-table-alias? {:table ref}))
       (is (not (requires-table-alias? {:table ref})))))))

(deftest providing-an-alias-satisfies-requirement
  (let [ccd {:table "a.a0123456789012345678901"}]
    (is (requires-table-alias? ccd))
    (is (not (requires-table-alias? (assoc ccd :table-alias "a01"))))))

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
      (gen/elements [:table :queue :queue-table])
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

(defspec ccds-with-table-names-over-22-chars-need-aliases
  (chuck/for-all
   [ccd gen-change-capture-def]
   (if (> (count (table-name ccd)) 22)
     (do
       (is (= ccd (validate-ccd ccd)))
       (is (nil? (validate-ccd (dissoc ccd :table-alias)))))
     (do
       (is (= ccd (validate-ccd ccd)))
       (is (validate-ccd (dissoc ccd :table-alias)))))))
