(ns work.core-test
  (:use clojure.test
	work.graph
	[plumbing.core :only [retry wait-until]]
	[plumbing.serialize :only [send-clj clj-worker
				   send-json json-worker]])
  (:require [work.core :as work])
  (:require [work.queue :as q]))

(deftest map-work-test
  (is (= (range 10 1010 10)
         (work/map-work #(* 10 %)
                        10
                        (range 1 101 1)))))

(defn wait-for-complete-results
  "Test helper fn waits until the pool finishes processing before returning results."
  [response-q expected-seq-size]
  (wait-until #(= (.size response-q) expected-seq-size) 20)
  (sort (iterator-seq (.iterator response-q))))

(deftest do-work-test
  (let [input-data (range 1 101 1)
        response-q (q/local-queue)
        pool (work/do-work #(q/offer response-q (* 10 %))
                           10
                           input-data)]
    (is (= (range 10 1010 10)
           (wait-for-complete-results response-q (count input-data))))))

(deftest reduce-work-test
  (is (= (range 10)
         (sort (work/reduce-work conj (range 10)))))
  (is (= nil
         (work/reduce-work + (list)))))

(deftest queue-work-test
  (let [input-data (range 1 101 1)
	input (q/local-queue input-data)
	output (q/local-queue)
	pool (future (work/queue-work
		      (work/work (fn [] {:f #(* 10 %)
		       :in #(q/poll input)
		       :out #(q/offer output %)}))))]
    (is (= (range 10 1010 10)
	   (wait-for-complete-results output (count input-data))))))

(deftest blocking-queue-work-test
  (let [input-data (range 1 21 1)
	request-q (q/local-queue input-data)
	response-q (q/local-queue)
	pool (future
	      (work/queue-work
	       (work/work (fn []
			    {:f #(do (Thread/sleep 1000) (* 10 %))
			     :in #(q/poll request-q)
			     :out #(q/offer response-q %)}))
	       10))]
    (is (= (range 10 210 10)
	   (wait-for-complete-results response-q (count input-data))))))

(deftest async-queue-work-test
  (let [input-data (range 1 101 1)
        request-q (q/local-queue input-data)
        response-q (q/local-queue)
        pool (future
              (work/queue-work
	       (work/work (fn []
               {:f (fn [task put-done]
                     (put-done (* 10 task)))
                :in #(q/poll request-q)
                :out #(q/offer response-q %)
                :exec work/async}))
	       10))]
    (is (= (range 10 1010 10)
           (wait-for-complete-results response-q (count input-data))))))

(deftest async-blocking-queue-work-test
  (let [input-data (range 1 21 1)
        request-q (q/local-queue input-data)
        response-q (q/local-queue)
        pool (future
              (work/queue-work
	       (work/work (fn []
			    {:f (fn [task put-done]
				  (Thread/sleep 1000)
				  (put-done (* 10 task)))
			     :in #(q/poll request-q)
			     :out #(q/offer response-q %)
			     :exec work/async}))
	       10))]
    (is (= (range 10 210 10)
           (wait-for-complete-results response-q (count input-data))))))

(defn times10 [x] (* 10 x))

(deftest clj-fns-over-the-queue
  (let [input-data (range 1 101 1)
	request-q (q/local-queue (map #(send-clj %1 %2)
					 (repeat #'times10)
					 input-data))
	response-q (q/local-queue)
	pool (future
	      (work/queue-work
	       (work/work (fn []
	       {:f clj-worker
	       :in #(q/poll request-q)
	       :out #(q/offer response-q %) }))
	       10))]
    (is (= (range 10 1010 10)
	   (wait-for-complete-results response-q (count input-data))))))

(deftest json-fns-over-the-queue
  (let [input-data (range 1 101 1)
	request-q (q/local-queue (map #(send-json %1 %2)
				      (repeat #'times10)
				      input-data))
	response-q (q/local-queue)
	pool (future
	      (work/queue-work
	       (work/work
		(fn []
		  {:f json-worker
		   :in #(q/poll request-q)
		   :out #(q/offer response-q %) }))
	       10))]
    (is (= (range 10 1010 10)
	   (wait-for-complete-results response-q (count input-data))))))

(deftest keyed-producer-consumer-test
  (let [[put-work get-work done-work]
	   (work/keyed-producer-consumer
	    (fn [k v1 v2]
	      (->> (concat v1 v2)
		   (into #{}))))]
    (put-work :u1 [:a :b])
    (is (= [:u1 #{:a :b}] (get-work)))
    (put-work :u1 [:c :d])
    (put-work :u2 [:e])
    (is (= [:u2 #{:e}] (get-work)))
    (done-work :u1)
    (is (= [:u1 #{:c :d}] (get-work)))))
