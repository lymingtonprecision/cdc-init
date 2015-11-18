(ns cdc-util.async
  (:require [clojure.core.async :as async]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public

(defn pipe-ret-last
  "Pipes items from `from-chan` to `to-chan` returning a channel that
  will receive the last value read from `from-chan` when it closes.

  If `close?` is `true`, the default, then `to-chan` will be closed
  when `from-chan` closes."
  ([from-chan to-chan]
   (pipe-ret-last from-chan to-chan true))
  ([from-chan to-chan close?]
  (async/go
    (loop [v nil]
      (let [u (async/<! from-chan)]
        (if (nil? u)
          (do
            (when close? (async/close! to-chan))
            v)
          (do
            (async/>! to-chan u)
            (recur u))))))))

(defn go-till-closed
  "Returns a `java.io.Closeable` encapsulated `go` block that takes
  values from the given `chans` (a single channel or collection of
  channels) providing them to the `loop-fn` when received until
  `.close`d.

  `loop-fn` should have the signature `(fn [val ch] ...)`, where `val`
  is the value read and `ch` is the channel read from.

  If `close?` is true, the default, then all of the provided channels
  will be closed when `.close` is called on the returned block."
  ([chans loop-fn]
   (go-till-closed chans loop-fn true))
  ([chans loop-fn close?]
   (let [close-chan (async/chan)
         data-chans (if (coll? chans) chans [chans])
         all-chans (conj data-chans close-chan)
         block (async/go-loop [chs all-chans]
                 (if (= [close-chan] chs)
                   (async/close! close-chan)
                   (let [[v ch] (async/alts! chs)]
                     (when-not (= close-chan ch)
                       (when v (loop-fn v ch))
                       (recur (if v chs (remove #(= ch %) chs)))))))]
     (reify java.io.Closeable
       (close [this]
         (async/close! close-chan)
         (when close? (doseq [ch data-chans] (async/close! ch)))
         (async/<!! block))))))

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
