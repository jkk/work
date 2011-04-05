(ns work.graph-test
  (:require [work.queue :as q]
	    [clojure.zip :as zip])
  (:use clojure.test
	plumbing.core
	plumbing.error
	work.graph))

;; ;;TODO: copied from core-test, need to factor out to somehwere.
(defn wait-for-complete-results
  "Test helper fn waits until the pool finishes processing before returning results."
  [response-q expected-seq-size]
  (wait-until #(= (.size response-q) expected-seq-size) 5)
  (iterator-seq (.iterator response-q)))

(deftest one-node-graph-test
  (let [incq (q/local-queue)
	idq (q/local-queue)
	root (-> (graph)
		 (each (out inc incq))
		 (each (out identity idq)))]
    (run-sync root (range 5))
    (is (= (range 1 6) (wait-for-complete-results incq 5)))
    (is (= (range 5) (wait-for-complete-results idq 5)))))

(deftest one-node-observer-test
  (let [a (atom 0)
	obs (fn [{:keys [f]}]
	      (fn [& args]
		(swap! a inc)
		(apply f args)))
	incq (q/local-queue)
	root (-> (graph)
		 (each (out inc incq)))]
    (run-sync root (range 5) obs)
    (is (= (range 1 6) (wait-for-complete-results incq 5)))
    (is (= 10 @a)))) ;;observes the root identity node and child.

(deftest multimap-graph-test
  (let [multiq (q/local-queue)
	root (-> (graph)
		 (multimap range)
		 >>
		 (each (out inc multiq)))]
    (run-sync root (range 4))
    (is (= [1 1 2 1 2 3]
	     (wait-for-complete-results multiq 5)))))

(deftest multimap-with-pred-test
  (let [multiq (q/local-queue)
	root (-> (graph)
		 (multimap range :when even?)
		 >>
		 (each (out inc multiq)))]
    (run-sync root (range 4))
    (is (= [1 2]
	     (wait-for-complete-results multiq 5)))))

(deftest chain-pipeline-test
  (let [incq (q/local-queue)
	root (-> (graph)
		 (each inc)
		 >>
		 (each inc)
		 >>
		 (each inc)
		 >>
		 (each (out inc incq)))]
    (run-sync root (range 5))
    (is (= (range 4 9) (wait-for-complete-results incq 5)))))

(deftest chain-multicast-pipeline-test
  (let [incq (q/local-queue)
	root (-> (graph)
		 (each inc)
		 >>
		 (each inc)
		 (each inc)
		 (each inc)
		 (multimap range)
		 >>
		 (each inc)
		 >>
		 (each (out inc incq)))]
    (run-sync root (range 5))
    (is (= [2 2 3 2 3 4 2 3 4 5 2 3 4 5 6] (wait-for-complete-results incq 5)))))

(deftest chain-graph-test
  (let [incq (q/local-queue)
	plus2q (q/local-queue)
	root (-> (graph)
		 (each inc)
		 >>
		 (each (out inc incq))
		 (each (out (partial + 2)
			     plus2q)))]
    (run-sync root (range 5))
    (is (= (range 2 7) (wait-for-complete-results incq 5)))
    (is (= (range 3 8) (wait-for-complete-results plus2q 5)))))

(deftest simple-predicate-test
  (let [incq (q/local-queue)
	idq (q/local-queue)
	root (-> (graph)
		 (each (out identity idq) :when even?)
		 (each (out inc incq) :when odd?))]
    (run-sync root (range 5))
    (is (= (range 2 5 2) (wait-for-complete-results incq 5)))
    (is (= (range 0 5 2) (wait-for-complete-results idq 5)))))

(deftest pool-test
  (let [incq (q/local-queue)
	idq (q/local-queue)
	root (-> (graph)
		 (each (out identity idq) :when even?)
		 (each (out inc incq) :when odd?))
	running (run-pool root 10 (q/local-queue (range 21)))]
    (is (= (range 2 21 2) (sort (wait-for-complete-results incq 5))))
    (is (= (range 0 21 2) (sort (wait-for-complete-results idq 5))))
    (kill-graph running)))

(deftest higher-order-observation
  (let [incq (q/local-queue)
	[a l] (atom-logger)
      root (-> (graph)
	       (each (out inc incq)))]
    (run-sync root [1 2 "fuck" 3] (fn [f] #(with-ex l f %)))
      (is (= [2 3 4] (sort (wait-for-complete-results incq 5))))
      (is (= "java.lang.ClassCastException: java.lang.String cannot be cast to java.lang.Number" @a))))