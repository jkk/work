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
    (is (= (range 1 6) (seq (sort incq))))
    (is (= (range 5) (seq (sort idq))))))

(deftest mulibranch-test
  (let [incq (q/local-queue)
	idq (q/local-queue)
	root (-> (graph)
		 (subgraph
		  (each inc)
		  >>
		  (each (out inc incq)))
		 (subgraph
		  (each identity)
		  >>
		  (each (out dec idq))))]
    (run-sync root (range 5))
    (is (= (range 2 7) (seq (sort incq))))
    (is (= (range -1 4) (seq (sort idq))))))

(deftest one-node-observer-test
  (let [a (atom 0)
	obs (fn [{:keys [f] :as vertex}]
	      (fn [& args]
		(swap! a inc)
		(apply f args)))
	incq (q/local-queue)
	root (-> (graph)
		 (each (out inc incq)))]
    (run-sync root (range 5) (partial observer-rewrite obs))
    (is (= (range 1 6) (seq (sort incq))))
    ;;observes the root identity node, child, and whole graph
    (is (= 10 @a)))) 

(deftest multimap-graph-test
  (let [multiq (q/local-queue)
	root (-> (graph)
		 (multimap range)
		 >>
		 (each (out inc multiq)))]
    (run-sync root (range 4))
    (is (= [1 1 1 2 2 3]
	     (sort (seq multiq))))))

(deftest append-child-graph-test
  (let [multiq (q/local-queue)
	root (-> (graph)
		 (multimap range
			   :id :hang-city)
		 >>
		 (each (out inc multiq)))]
    (publish :hang-city {:topic "foo" :id "bar"} root)
    (run-sync root (range 4))
    (is (= [1 1 1 2 2 3]
	     (sort (seq multiq))))))

(deftest multimap-with-pred-test
  (let [multiq (q/local-queue)
	root (-> (graph)
		 (multimap range :when even?)
		 >>
		 (each (out inc multiq)))]
    (run-sync root (range 4))
    (is (= [1 2]
	     (sort (seq multiq))))))

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
    (is (= (range 4 9) (sort (seq incq))))))

(defn root-graph [tail]
  (-> (graph)
      (each inc)
      >>
      (each inc)
      >>
      (each inc)
      >>
      (each tail)))

(deftest inline-graph-test
  (let [incq (q/local-queue)
	root (-> (graph)
		 (inline-graph (root-graph inc))
		 (each (out inc incq)))]
    (run-sync root (range 5))
    (is (= (range 5 10) (sort (seq incq))))))

(deftest inline-prefix-graph-test
  (let [incq (q/local-queue)
	root (-> (graph)
		 (each inc)
		 >>
		 (inline-graph (root-graph inc))
		 (each (out inc incq)))]
    (run-sync root (range 5))
    (is (= (range 6 11) (sort (seq incq))))))

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
    (is (= [2 2 3 2 3 4 2 3 4 5 2 3 4 5 6]
	     (seq incq)))))

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
    (is (= (range 2 7) (seq incq)))
    (is (= (range 3 8) (seq plus2q)))))


(deftest queue-uniquess-test
  (let [s  (atom #{})
	g (-> (graph)
	      (each (fn [x]
		      (when (@s x)
			(println "ALREADY SAW YOU"))
		      (swap! s conj x))
		    :threads 10))
	root (run-pool g
		  fifo-in)]
    (q/offer-all (:queue root) (range 1e6))
    (wait-until #(= (count @s) 1e6) 10)
    (is (= (count @s) 1e6))
    (kill-graph root)))

(deftest simple-predicate-test
  (let [incq (q/local-queue)
	idq (q/local-queue)
	root (-> (graph)
		 (each (out identity idq) :when even?)
		 (each (out inc incq) :when odd?))]
    (run-sync root (range 5))
    (is (= (range 2 5 2) (seq incq)))
    (is (= (range 0 5 2) (seq idq)))))

(deftest pool-test
  (let [incq (q/local-queue)
	idq (q/local-queue)
	root (-> (graph)
		 (each (out identity idq) :when even?)
		 (each (out inc incq) :when odd?))
	running (run-pool root comp-rewrite)]
    (q/offer-all (:queue running) (range 5))
    (is (= (range 2 5 2) (sort (wait-for-complete-results incq 2))))
    (is (= (range 0 5 2) (sort (wait-for-complete-results idq 3))))
    (kill-graph running)))

(deftest higher-order-observation
  (let [incq (q/local-queue)
	[a l] (atom-logger)
      root (-> (graph)
	       (each (out inc incq)))]
    (run-sync root [1 2 "fuck" 3]
	      (partial observer-rewrite (fn [f] #(with-ex l f %))))
      (is (= [2 3 4] (sort (wait-for-complete-results incq 5))))
      (is (= "java.lang.ClassCastException: java.lang.String cannot be cast to java.lang.Number" @a))))

(deftest priority-in-test
  (let [{:keys [in offer queue f]} (priority-in 5 {:f identity})]
    (offer 10)
    (offer {:item 2 :priority 10})
    (offer {:item 3 :priority 1})
    (is (= 2 (f (in))))
    (is (= 10 (:item (in))))
    (is (= 3 (f (in))))))