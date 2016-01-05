(ns cdc-util.schema.oracle-refs-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [com.gfredericks.test.chuck.clojure-test :as chuck]
            [com.gfredericks.test.chuck.generators :as chuck.gen]

            [clojure.string :as string]
            [schema.core :as schema]

            [cdc-util.schema.oracle-refs :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Generic Generators

(defn gen-quoted-ref
  ([]
   (gen-quoted-ref 30))
  ([max-len]
   (chuck.gen/string-from-regex
    (re-pattern (str "\"[\\s[^\"] ]{1," max-len "}\"")))))

(defn gen-unquoted-ref
  ([]
   (gen-unquoted-ref 30))
  ([max-len]
   (chuck.gen/string-from-regex
    (re-pattern (str "[a-z][a-z0-9_$#]{0," (dec max-len) "}")))))

(defn gen-ref
  ([]
   (gen-ref 30))
  ([max-len]
   (gen/one-of [(gen-quoted-ref max-len) (gen-unquoted-ref max-len)])))

(def gen-invalid-ref
  (gen/one-of
   [(chuck.gen/string-from-regex #"\"[\\s\" ]*\"[a-z0-9]+")
    (chuck.gen/string-from-regex #"[0-9_$#][a-z0-9_$#]*")
    (chuck.gen/string-from-regex #"[a-z][^a-z0-9_$#]+.*")]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; db-schema-ref

(def gen-schema-ref
  (gen/fmap
   #(string/join "." %)
   (gen/tuple (gen-ref) (gen-ref))))

(def gen-invalid-schema-ref
  (gen/fmap
   (fn [[f & refs]] (string/join "." (f refs)))
   (gen/tuple
    (gen/elements [identity reverse])
    gen-invalid-ref
    (gen/frequency [[1 gen-schema-ref] [2 gen-invalid-ref]]))))

(defspec db-schema-refs
  (chuck/for-all
   [v gen-schema-ref]
   (is (nil? (schema/check db-schema-ref v)))))

(defspec invalid-db-schema-refs
  (chuck/for-all
   [v gen-invalid-schema-ref]
   (is (schema/check db-schema-ref v))))

(deftest db-schema-refs-must-be-30-chars-or-less
  (is (schema/check db-schema-ref (str (string/join (repeat 31 "a")) ".abc")))
  (is (schema/check db-schema-ref (str "abc." (string/join (repeat 31 "a")))))
  (is (nil? (schema/check db-schema-ref (str "abc." (string/join (repeat 30 "a")))))))

(deftest unquoted-db-schema-refs-must-start-with-alpha
  (is (schema/check db-schema-ref (str "abc.0def"))))

(deftest unquoted-db-schema-refs-cant-contain-non-alphanumerics
  (let [s (str "a-b-c.d!f")]
    (is (schema/check db-schema-ref s))
    (is (nil? (schema/check db-schema-ref (string/replace (str "\"" s "\"") #"\." "\".\""))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; queue-ref

(def gen-queue-ref
  (gen/fmap
   #(string/join "." %)
   (gen/tuple
    (gen-ref)
    (gen/one-of [(gen-quoted-ref 24) (gen-unquoted-ref 24)]))))

(defspec queue-refs
  (chuck/for-all
   [v gen-queue-ref]
   (is (nil? (schema/check queue-ref v)))))

(defspec invalid-queue-refs
  (chuck/for-all
   [v gen-invalid-schema-ref]
   (is (schema/check queue-ref v))))

(deftest queue-refs-must-be-24-chars-or-less
  (is (schema/check queue-ref (str "a.\"" (string/join (repeat 25 "a")) \")))
  (is (nil? (schema/check queue-ref (str "a.\"" (string/join (repeat 24 "a")) \"))))
  (is (schema/check queue-ref (str "a." (string/join (repeat 25 "a")))))
  (is (nil? (schema/check queue-ref (str "a." (string/join (repeat 24 "a")))))))

(deftest queue-refs-must-have-schemas
  (is (schema/check queue-ref "a"))
  (is (nil? (schema/check queue-ref "a.a"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; table-alias

(def gen-table-alias
  (gen/one-of [(gen-quoted-ref 22) (gen-unquoted-ref 22)]))

(defspec valid-table-alias
  (chuck/for-all
   [v gen-table-alias]
   (is (nil? (schema/check table-alias v)))))

(defspec invalid-table-alias
  (chuck/for-all
   [v gen-invalid-ref]
   (is (schema/check table-alias v))))

(deftest table-alias-refs-must-be-22-chars-or-less
  (is (schema/check table-alias (str \" (string/join (repeat 23 "a")) \")))
  (is (schema/check table-alias (string/join (repeat 23 "a"))))
  (is (nil? (schema/check table-alias (string/join (repeat 22 "a"))))))
