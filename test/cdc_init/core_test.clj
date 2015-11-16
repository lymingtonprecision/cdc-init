(ns cdc-init.core-test
  (:require [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck.clojure-test :as chuck]

            [clojure.core.async :as async]
            [clj-time.core :as time]

            [cdc-init.test-dummies :refer :all]
            [cdc-init.test-generators :refer :all]
            [cdc-init.protocols :refer :all]
            [cdc-init.core :refer :all]))

(use-fixtures :each reset-dummies!)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; utility fns

(defn wait-for [c]
  (loop []
    (when (not (.closed? c))
      (async/<!! (async/timeout 5))
      (recur))))

(defn <!!last [c]
  (let [l (atom nil)]
    (loop []
      (when-let [v (async/<!! c)]
        (reset! l v)
        (recur)))
    @l))

(defn <!!all [c]
  (async/<!! (async/into [] c)))

(defn prepare-after [f]
  (let [ccd (new-change-capture-def)]
    (f ccd)
    (prepare ccd *dummy-database* *dummy-kafka*)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; prepare

(def ccd-preparation-states
  (sort [:prepared :queue-created :topic-created :trigger-created]))

(defspec preparation
  (chuck/for-all
   [start-time (gen/return (time/now))
    ccd (gen/fmap #(<!!last (prepare % *dummy-database* *dummy-kafka*))
                  gen-change-capture-def)]
   (is (queue-exists? *dummy-database* (:queue ccd)))
   (is (trigger-exists? *dummy-database* (:table ccd)))
   (is (topic-exists? *dummy-kafka* (:queue ccd)))
   (is (= :prepared (:status ccd)))
   (is (time/within? (time/interval
                      start-time
                      (time/plus (time/now) (time/millis 1)))
                     (:timestamp ccd)))))

(deftest preparation-records-actions
  (let [ccd (new-change-capture-def)
        states (<!!all (prepare ccd *dummy-database* *dummy-kafka*))]
    (is (= ccd-preparation-states (sort (map :status states))))))

(defspec preparation-only-records-required-actions
  (chuck/for-all
   [ccd gen-change-capture-def
    e (gen/elements [:queue :trigger :topic])]
   (reset-dummies!)
   (case e
     :queue (create-queue! *dummy-database* (:queue ccd) (:queue-table ccd))
     :trigger (create-trigger! *dummy-database* (:table ccd) (:queue ccd) (:trigger ccd))
     :topic (create-topic! *dummy-kafka* (:queue ccd)))
   (let [exp (sort (remove #(.startsWith (str %) (str e)) ccd-preparation-states))
         states (<!!all (prepare ccd *dummy-database* *dummy-kafka*))]
     (is (= exp (sort (map :status states)))))))

(deftest preparation-clears-queue
  (let [ccd (<!!last
             (prepare-after
              (fn [ccd]
                (create-queue! *dummy-database* (:queue ccd) (:queue-table ccd))
                (swap! (:queues *dummy-database*)
                       (fn [qs]
                         (assoc qs (:queue ccd) (doall (map rand-int (range 1 10)))))))))]
    (is (= :prepared (:status ccd)))
    (is (queue-exists? *dummy-database* (:queue ccd)))
    (is (empty? (-> *dummy-database* :queues deref (get (:queue ccd)))))))

(deftest preparation-clears-topic
  (let [ccd (<!!last
             (prepare-after
              (fn [ccd]
                (create-topic! *dummy-kafka* (:queue ccd))
                (swap! (:topics *dummy-kafka*)
                       (fn [qs]
                         (assoc qs (:queue ccd) (doall (map rand-int (range 1 10)))))))))]
    (is (= :prepared (:status ccd)))
    (is (topic-exists? *dummy-kafka* (:queue ccd)))
    (is (empty? (-> *dummy-kafka* :topics deref (get (:queue ccd)))))))

(deftest preparation-disables-trigger
  (let [ccd (<!!last
             (prepare-after
              (fn [ccd]
                (create-trigger! *dummy-database* (:table ccd) (:queue ccd) (:trigger ccd))
                (enable-trigger! *dummy-database* (:table ccd)))))]
    (is (= :prepared (:status ccd)))
    (is (trigger-exists? *dummy-database* (:table ccd)))
    (is (false? (-> *dummy-database* :triggers deref (get (:table ccd)))))))

(defspec preparation-captures-errors
  (chuck/for-all
   [ccd gen-change-capture-def
    evt (gen/elements [:create-queue :create-trigger :create-topic])]
   (do
     (reset-dummies!)
     (error-on! (if (= :create-topic evt) *dummy-kafka* *dummy-database*) evt))
   (let [res (<!!last (prepare ccd *dummy-database* *dummy-kafka*))]
     (is (= :error (:status res)))
     (is (instance? Exception (:error res))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; initialize

(defspec initialize-fills-topic-with-seeds
  20
  (chuck/for-all
   [ccd gen-change-capture-def
    seeds gen-seeds]
   (do
     (reset-dummies!)
     (swap! (:seeds *dummy-seeds*) assoc (:table ccd) seeds)
     (wait-for (prepare ccd *dummy-database* *dummy-kafka*))
     (wait-for (initialize ccd *dummy-kafka* *dummy-seeds* *dummy-database*)))
   (is (= seeds (get @(:topics *dummy-kafka*) (:queue ccd))))))

(defspec initialize-handles-map-and-non-map-seeds
  20
  (chuck/for-all
   [ccd gen-change-capture-def
    seeds (gen/vector
           (gen/frequency
            [[1 gen-seed]
             [1 gen-seed-value]]))]
   (do
     (reset-dummies!)
     (swap! (:seeds *dummy-seeds*) assoc (:table ccd) seeds)
     (wait-for (prepare ccd *dummy-database* *dummy-kafka*))
     (wait-for (initialize ccd *dummy-kafka* *dummy-seeds* *dummy-database*)))
   (let [exp (map (fn [s] (if (map? s) s {:key nil :value s})) seeds)]
     (is (= exp (get @(:topics *dummy-kafka*) (:queue ccd)))))))

(defspec initialize-ends-in-active-status
  20
  (chuck/for-all
   [ccd gen-change-capture-def
    seeds gen-seeds]
   (do
     (reset-dummies!)
     (swap! (:seeds *dummy-seeds*) assoc (:table ccd) seeds)
     (wait-for (prepare ccd *dummy-database* *dummy-kafka*)))
   (is (= :active
          (-> (initialize ccd *dummy-kafka* *dummy-seeds* *dummy-database*)
              <!!last
              (get :status))))))

(defspec initialize-enables-trigger
  10
  (chuck/for-all
   [ccd gen-change-capture-def]
   (do
     (reset-dummies!)
     (wait-for (prepare ccd *dummy-database* *dummy-kafka*))
     (wait-for (initialize ccd *dummy-kafka* *dummy-seeds* *dummy-database*)))
   (is (trigger-enabled? *dummy-database* (:table ccd)))))

(deftest initialize-disables-trigger-on-error
  (let [ccd (new-change-capture-def)]
    (wait-for (prepare ccd *dummy-database* *dummy-kafka*))
    (error-on! *dummy-seeds* :to-chan)
    (wait-for (initialize ccd *dummy-kafka* *dummy-seeds* *dummy-database*))
    (is (not (trigger-enabled? *dummy-database* (:table ccd))))))

(defspec initialize-captures-send-errors
  (chuck/for-all
   [ccd gen-change-capture-def
    seeds (gen/not-empty gen-seeds)]
   (let [i (rand-int (dec (count seeds)))]
     (reset-dummies!)
     (wait-for (prepare ccd *dummy-database* *dummy-kafka*))
     (swap! (:seeds *dummy-seeds*) assoc (:table ccd) seeds)
     (error-on! *dummy-kafka* :send
                (fn [_ _ _ m]
                  (when (>= (count m) i)
                    (throw (Exception. "topic full"))))))
   (let [ccd (<!!last
              (initialize ccd *dummy-kafka* *dummy-seeds* *dummy-database*))]
     (is (= :error (:status ccd)))
     (is (instance? Exception (:error ccd))))))

(defn- progress-variant? [[c t]]
  (and c t
       (or (zero? c) (pos? c))
       (pos? t)))

(deftest initialize-reports-seeding-progress
  (let [seeds (sample-seeds 100)
        ccd (<!!last
             (prepare-after
              (fn [ccd]
                (swap! (:seeds *dummy-seeds*) assoc (:table ccd) seeds))))
        states (butlast
                (<!!all
                 (initialize ccd *dummy-kafka* *dummy-seeds* *dummy-database*)))]
    (testing "returns at least one non-active state"
      (is (> (count states) 0)))
    (testing "returned states are seeding updates"
      (is (every? #(= :seeding (:status %)) states))
      (is (every? progress-variant? (map :progress states))))
    (testing "all progress updates have correct total"
      (is (every? #(= (count seeds) %) (map #(last (:progress %)) states))))
    (testing "returns at least one seeding state with a progress count"
      (is (some pos? (map #(first (:progress %)) states))))
    (testing "progress counts increase across updates"
      (is (every? (fn [[x y]] (< x y)) (partition 2 1 (map #(first (:progress %)) states)))))))

(deftest initialize-from-no-seeds-reports-no-progress
  (let [ccd (wait-for
             (prepare (new-change-capture-def) *dummy-database* *dummy-kafka*))
        states (<!!all
                (initialize ccd *dummy-kafka* *dummy-seeds* *dummy-database*))]
    (is (not-any? #(= :seeding (:status %)) states))))

(deftest initialize-reports-progress-every-two-pcnt-at-most
  (let [seeds (sample-seeds 1000)
        ccd (<!!last
             (prepare-after
              (fn [ccd]
                (swap! (:seeds *dummy-seeds*) assoc (:table ccd) seeds))))
        progress-updates (filter
                          #(> (-> % :progress first) 0)
                          (butlast
                           (<!!all
                            (initialize ccd *dummy-kafka* *dummy-seeds* *dummy-database*))))]
    (is (<= (count progress-updates) 50))))
