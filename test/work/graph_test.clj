(ns work.graph-test
  (:require [work.queue :as q]
	    [clojure.zip :as zip])
  (:use clojure.test
	plumbing.core
	work.graph))

;;TODO: copied from core-test, need to factor out to somehwere.
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

(deftest multimap-graph-test
  (let [multiq (q/local-queue)
	root (-> (graph)
		 (multimap range)
		 >>
		 (each (out inc multiq)))]
    (run-sync root (range 4))
    (is (= [1 1 2 1 2 3]
	     (wait-for-complete-results multiq 5)))))

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