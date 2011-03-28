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

(deftest graph-comp-test
  (let [inc-graph
	{:f inc
	 :children [{:f inc} {:f identity}]}]
    (is (= [5 4] ((graph-comp inc-graph) 3)))))

(deftest one-node-graph-test
  (let [incq (q/local-queue)
	idq (q/local-queue)
	root (-> (root-node)
		 graph-zip
		 (child (node (out inc incq)))
		 (child (node (out identity idq))))]
    (mono-run root (range 5))
    (is (= (range 1 6) (wait-for-complete-results incq 5)))
    (is (= (range 5) (wait-for-complete-results idq 5)))))

(deftest multimap-graph-test
  (let [multiq (q/local-queue)
	root (-> (root-node)
		 graph-zip
		 (child-> (multimap range))
		 (child (node (out inc multiq))))]
    (mono-run root (range 4))
    (is (= [1 1 2 1 2 3]
	     (wait-for-complete-results multiq 5)))))

(deftest chain-graph-test
  (let [incq (q/local-queue)
	plus2q (q/local-queue)
	root (-> (root-node)
		 graph-zip
		 (child-> (node inc))
		 (child (node (out inc incq)))
		 (child (node (out (partial + 2)
				   plus2q))))]
    (mono-run root (range 5))
    (is (= (range 2 7) (wait-for-complete-results incq 5)))
    (is (= (range 3 8) (wait-for-complete-results plus2q 5)))))

(deftest simple-predicate-test
  (let [incq (q/local-queue)
	idq (q/local-queue)
	root (-> (root-node)
		 graph-zip
		 (child (node (out identity idq) :when even?))
		 (child (node (out inc incq) :when odd?)))]
    (mono-run root (range 5))
    (is (= (range 2 5 2) (wait-for-complete-results incq 5)))
    (is (= (range 0 5 2) (wait-for-complete-results idq 5)))))
