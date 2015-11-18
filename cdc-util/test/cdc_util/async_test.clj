(ns cdc-util.async-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [cdc-util.async :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; utility fns

(defn rand-int-vec
  ([] (rand-int-vec 10))
  ([n] (rand-int-vec n 10000))
  ([n s] (take n (repeatedly #(rand-int s)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; pipe-ret-last

(deftest pipe-ret-last-pipes-values
  (let [vals (rand-int-vec)
        to-chan (async/chan)
        from-chan (async/to-chan vals)]
    (pipe-ret-last from-chan to-chan)
    (is (= vals
           (loop [rs []]
             (let [v (async/<!! to-chan)]
               (if (nil? v)
                 rs
                 (recur (conj rs v)))))))))

(deftest pipe-ret-last-returns-final-value
  (let [vals (rand-int-vec)
        to-chan (async/chan (async/sliding-buffer 1))
        from-chan (async/to-chan vals)]
    (is (= (last vals) (async/<!! (pipe-ret-last from-chan to-chan))))))

(deftest pipe-ret-last-returns-nil-on-no-values
  (let [to-chan (async/chan (async/sliding-buffer 1))
        from-chan (async/to-chan [])]
    (is (nil? (async/<!! (pipe-ret-last from-chan to-chan))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; go-till-closed

(deftest go-till-closed-processes-single-chan
  (let [vals (rand-int-vec)
        ch (async/to-chan vals)
        act (atom [])
        block (go-till-closed ch (fn [v _] (swap! act conj v)))]
    (loop [] (when-not (= (count vals) (count @act)) (recur)))
    (is (= vals @act))))

(deftest go-till-closed-processes-multiple-chans
  (let [vals [(range 90 110) (range 100 140) (range 130 150)]
        total (count (flatten vals))
        exp (sort (flatten vals))
        chs (map async/to-chan vals)
        act (atom [])
        block (go-till-closed chs (fn [v _] (swap! act conj v)))]
    (loop [] (when-not (= total (count @act)) (recur)))
    (is (= exp (sort @act)))))

(deftest go-till-closed-closes-inputs
  (let [ch (async/to-chan (repeatedly #(rand-int 100000)))
        vals-received (atom 0)
        block (go-till-closed ch (fn [_ _] (swap! vals-received inc)))]
    (loop [] (when (< 5 @vals-received) (recur)))
    (.close block)
    (is (.closed? ch))))
