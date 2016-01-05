(ns cdc-util.format
  (:require [cheshire.core :as cheshire]
            [cheshire.generate :as cheshire.gen]
            [clj-time.format :as time.format]
            [schema.utils]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Globals

(def iso8601 (time.format/formatters :basic-date-time))

(cheshire.gen/add-encoder
 org.joda.time.DateTime
 (fn [t json]
   (.writeString json (time.format/unparse iso8601 t))))

(cheshire.gen/add-encoder
 java.lang.Exception
 (fn [ex json]
   (cheshire.gen/encode-map
    {:type (.getName (class ex))
     :message (.getMessage ex)}
    json)))

(cheshire.gen/add-encoder
 schema.utils.ValidationError
 (fn [err json]
   (.writeString json (pr-str err))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; utility fns

(def field-parsers
  {:status keyword
   :timestamp #(time.format/parse iso8601 %)})

(defn apply-parsers [ccd]
  (reduce
   (fn [ccd [field parser]]
     (if-let [v (get ccd field)]
       (assoc ccd field (parser v))
       ccd))
   ccd
   field-parsers))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn json-str->ccd
  "Applies additional transformations on a raw JSON map to convert the
  structure/values into a Change Capture Definition."
  [json]
  (apply-parsers (cheshire/parse-string json true)))

(defn ccd->json-str
  [ccd]
  (cheshire/generate-string ccd))

(defn json-decoder
  "Returns a Kafka `Decoder` that decodes the content as a UTF8
  encoded JSON string (returning a keywordized map.)"
  []
  (reify
    kafka.serializer.Decoder
    (fromBytes [this bytes]
      (cheshire/parse-string (String. bytes "UTF8") true))))

(defn ccd-decoder
  "Returns a Kafka `Decoder` that decodes the content as a UTF8
  encoded JSON string representation of a Change Data Capture
  Definition, returning the Change Capture Definition map."
  []
  (reify
    kafka.serializer.Decoder
    (fromBytes [this bytes]
      (json-str->ccd (String. bytes "UTF8")))))

(defn msg->ccd
  "Converts a Kafak `MessageAndMetadata` instance into a Change
  Capture Definition map."
  [^kafka.message.MessageAndMetadata msg]
  (json-str->ccd (String. (.message msg) "UTF8")))
