(ns cdc-init.async.util
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
