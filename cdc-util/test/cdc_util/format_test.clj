(ns cdc-util.format-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck.clojure-test :as chuck]

            [cheshire.core :as cheshire]
            [clj-kafka.consumer.zk :as kafka.consumer]
            [clj-time.format :as time.format]

            [cdc-util.test-generators :refer :all]
            [cdc-util.format :refer :all])
  (:import [kafka.message Message MessageAndMetadata]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; encoding

(deftest encodes-timestamps-as-iso8601-strings
  (let [ccd (new-change-capture-def)
        json (cheshire/generate-string ccd)
        raw (cheshire/parse-string json true)]
    (is (instance? String (:timestamp raw)))
    (is (= (:timestamp ccd)
           (time.format/parse
            (time.format/formatters :basic-date-time)
            (:timestamp raw))))))

(defspec encoding-roundtrip
  (chuck/for-all
   [ccd gen-change-capture-def]
   (is (= ccd (json-str->ccd (ccd->json-str ccd))))))

(deftest encodes-exceptions
  (let [msg (first (gen/sample (gen/resize (rand-int 100) gen/string-ascii)))]
    (is (= {:error {:type "java.lang.Exception"
                    :message msg}}
           (cheshire/parse-string
            (cheshire/generate-string {:error (Exception. msg)})
            true)))))

(deftest encodes-validation-errors-as-strings
  (let [err (schema/check s/Str 1)]
    (is (= (pr-str err)
           (cheshire/parse-string
            (cheshire/generate-string err)
            true)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ccd->msg / msg->ccd

(defspec decodes-status-as-keyword
  (chuck/for-all
   [msg (gen/fmap #(apply ccd->msg %)
                  (gen/tuple gen-change-capture-def gen/string-ascii gen/nat))]
   (is (keyword? (:status (.message msg))))))

(defspec decodes-timestamp-as-datetime
  (chuck/for-all
   [msg (gen/fmap #(apply ccd->msg %)
                  (gen/tuple gen-change-capture-def gen/string-ascii gen/nat))]
   (is (instance? org.joda.time.DateTime (:timestamp (.message msg))))))
