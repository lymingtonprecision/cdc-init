(ns cdc-init.util)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Validity helpers

(defn- valid-reference? [r]
  (and (instance? String r)
       (not (clojure.string/blank? r))))

(defn- every-reference-valid? [ccd]
  (every? valid-reference?
          (map #(get ccd %) [:table :queue :queue-table :trigger])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn validate-ccd
  "Returns the provided Change Capture Definition record if it
  conforms to the expected schema otherwise returns `nil`."
  [ccd]
  (some-> ccd
          (#(when (every-reference-valid? %) %))
          (#(when (instance? org.joda.time.DateTime (:timestamp %)) %))))

(defn noop-transducer
  "Returns a transducer that does nothing, returning the result set
  unchanged in response to any input (and passing through
  completion/init arity calls.)

  If a function, `f`, is provided it will be called with each received
  input value (and only the input value) but its return value will be
  ignored.

  Note that this *will* short circuit a transducer composition:

      (into
       []
       (comp (map #(* % 2)) (noop-transducer))
       (range 1 10))
      ;=> []

  Therefore it should only be used in situations where discarding
  every input and returning the initial value is the desired
  behaviour. The primary use case is performing an operation on values
  received by a `core.async` channel:

      ;; log every value received on a channel, but never put the
      ;; values on the channel
      (def ch (async/chan 1 (noop-transducer #(clojure.tools.logging/info %))))
      (async/>!! ch \"test\")
      (async/>!! ch {:logging true :updating false})
      (async/close! ch)

  Useful primarily in situations where you want to use a channel as a
  (blocking) pipe to some external system but only care about the
  operation being performed and not capturing a return
  value (e.g. updating a database, sending emails, posting to Apache
  Kafka, etc.)"
  ([]
   (noop-transducer (constantly nil)))
  ([f]
   (fn [xf]
     (fn
       ([] (xf))
       ([rs] (xf rs))
       ([rs v] (f v) rs)))))
