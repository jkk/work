(ns work.graph-test
  (:require [work.queue :as q]
	    [work.message :as message]
	    [clojure.zip :as zip]
            [clojure.contrib.zip-filter :as zf]
            [plumbing.observer :as obs]
            )
  (:use clojure.test
	plumbing.core
	plumbing.error
	work.graph
	[store.core :only [bucket-keys]]
	[store.api :only [store]]
	[ring.adapter.jetty :only [run-jetty]]
	[compojure.core :only [routes]]
	[store.net :only [rest-store-handler]]
	services.core))

;; ;;TODO: copied from core-test, need to factor out to somehwere.
(defn wait-for-complete-results
  "Test helper fn waits until the pool finishes processing before returning results."
  [response-q expected-seq-size]
  (wait-until #(= (.size response-q) expected-seq-size) 5)
  (iterator-seq (.iterator response-q)))

(defn filter-nodes [root pred]
  (->> root graph-zip zf/descendants
       (filter (comp pred zip/node))
       (map zip/node)))

(defn dag-ids [root]
  (->> root graph-zip zf/descendants
       (map (comp :id zip/node))))

(deftest update-node-test
  (let [g1 (-> (graph)
	       (each inc :id :foo)
	       >>
	       (each inc :id :bar)
	       (update-loc
		:bar #(add-child % (node inc :id :child)))
	       zip/root)
	bar  (-> g1
		 (filter-nodes #(-> % :id (= :bar)))
		 first)]
    (is (= [:root :foo :bar :child] (dag-ids g1)))
    (is (= [:child] (map :id (:children bar))))))

(deftest append-child-head-test
  (let [g1 (-> (graph)
	       (each inc :id :foo)
	       >>
	       (each inc :id :bar)
	       (append-child
		:foo {:id :child :f inc}))
	foo  (-> g1 zip/root
		 (filter-nodes #(-> % :id (= :foo)))
		 first)]
    
    (is (= [:child :bar] (map :id (:children foo))))))

(deftest append-child-tail-test
  (let [g1 (-> (graph)
	       (each inc :id :foo)
	       >>
	       (each inc :id :bar)
	       (append-child
		:bar {:id :child :f inc}))
	foo  (-> g1 zip/root
		 (filter-nodes #(-> % :id (= :foo)))
		 first)
	bar  (-> g1 zip/root
		 (filter-nodes #(-> % :id (= :bar)))
		 first)]
    
    (is (= [:bar] (map :id (:children foo))))
    (is (= [:child] (map :id (:children bar))))))

(deftest one-node-graph-test
  (let [incq (q/local-queue)
	idq (q/local-queue)
	root (-> (graph)
		 (each (out inc incq))
		 (each (out identity idq))
		 zip/root)]    
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
		  (each (out dec idq)))
		 zip/root)]
    (run-sync root (range 5))
    (is (= (range 2 7) (seq (sort incq))))
    (is (= (range -1 4) (seq (sort idq))))))

(defn test-observer []
  (let [a (atom {})]
    [a (obs/make-simple-observer #(swap! a update-in [%1] conj %2))]))

(deftest one-node-observer-test
  (let [[a o] (test-observer)
	incq (q/local-queue)
	root (-> (graph)
		 (each (out inc incq))
		 zip/root
		 (observer-rewrite o))]
    (run-sync root (range 5))
    (is (= (range 1 6) (seq (sort incq))))
    ;;observes the root identity node, child, and whole graph
    (is (= [1 1 1 1 1] (get @a ["root" :count])))
    (is (= [1 1 1 1 1] (get @a ["lambda" :count])))))

(deftest multimap-graph-test
  (let [multiq (q/local-queue)
	root (-> (graph)
		 (multimap range)
		 >>
		 (each (out inc multiq))
		 zip/root)]
    (run-sync root (range 4))
    (is (= [1 1 1 2 2 3]
	     (sort (seq multiq))))))

(def broker-spec
     {:remote {:host "localhost"
	       :port 4445
	       :type :rest}
      :subscriber {:host "localhost"
	      :port 4455
	      :type :rest
	      :id "service-id"}})

(defn queue-store [s spec]
  (run-jetty
   (apply routes (rest-store-handler s))         
   (assoc spec :join? false))
     s)

(deftest rest-broker-system-test
  (let [s (->  (store [] {:type :mem})
	       (queue-store (:remote broker-spec)))
	b (message/sub-broker broker-spec)
	pending (-> {:f identity}
		    (priority-in 10)
		     (subscribe b {:id "bar" :topic "foo"}))
	_ (message/start-subscribers b)
	in (:in pending)
	multiq (q/local-queue)
	pub-b (message/pub-broker broker-spec)
	root (-> (graph)
		 (multimap range
			   :id :hang-city)
		 >>
		 (each (out inc multiq))
		 (publish pub-b :hang-city
			  {:topic "foo" :id "blaz"})
		 zip/root)]
    (is (=  [["service-id"
	      {"host" "localhost"
		"port" 4455
		"type" "rest"
		"topic" "foo"
		"uri" "/foo"
		"id" "service-id"}]]
	      (map (fn [[k v]]
		     [k (select-keys v ["host" "topic" "port" "type" "uri" "id"])])
		   (s :seq "foo"))))
    (run-sync root (range 4))
    (is (= [1 1 1 2 2 3]
	     (sort (seq multiq))))
    (is (= ["foo"] (bucket-keys (.bucket-map s))))
    (is (= (:item (in)) 0))
    (is (= (:item  (in)) 2))))

(deftest multimap-with-pred-test
  (let [multiq (q/local-queue)
	root (-> (graph)
		 (multimap range :when even?)
		 >>
		 (each (out inc multiq))
		 zip/root)]
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
		 (each (out inc incq))
		 zip/root)]
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
		 (each (out inc incq))
		 zip/root)]
    (run-sync root (range 5))
    (is (= (range 5 10) (sort (seq incq))))))

(deftest inline-prefix-graph-test
  (let [incq (q/local-queue)
	root (-> (graph)
		 (each inc)
		 >>
		 (inline-graph (root-graph inc))
		 (each (out inc incq))
		 zip/root)]
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
		 (each (out inc incq))
		 zip/root)]
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
			    plus2q))
		 zip/root)]
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
		    :threads 10)
	      zip/root)
	root (run-pool g)]
    (q/offer-all (:queue root) (range 1e6))
    (wait-until #(= (count @s) 1e6) 10)
    (is (= (count @s) 1e6))
    (kill-graph root)))

(deftest simple-predicate-test
  (let [incq (q/local-queue)
	idq (q/local-queue)
	root (-> (graph)
		 (each (out identity idq) :when even?)
		 (each (out inc incq) :when odd?)
		 zip/root)]
    (run-sync root (range 5))
    (is (= (range 2 5 2) (seq incq)))
    (is (= (range 0 5 2) (seq idq)))))

(deftest pool-test
  (let [incq (q/local-queue)
	idq (q/local-queue)
	root (-> (graph)
		 (each (out identity idq) :when even?)
		 (each (out inc incq) :when odd?)
		 zip/root)
	running (run-pool root)]
    (q/offer-all (:queue running) (range 5))
    (is (= (range 2 5 2) (sort (wait-for-complete-results incq 2))))
    (is (= (range 0 5 2) (sort (wait-for-complete-results idq 3))))
    (kill-graph running)))


(deftest one-node-observer-test
  (let [[a o] (test-observer)
	incq (q/local-queue)
	root (-> (graph)
		 (each (out inc incq))
		 zip/root
		 (observer-rewrite o))]
    (run-sync root (range 5))
    (is (= (range 1 6) (seq (sort incq))))
    ;;observes the root identity node, child, and whole graph
    (is (= [1 1 1 1 1] (get @a ["root" :count])))
    (is (= [1 1 1 1 1] (get @a ["lambda" :count])))))

(deftest higher-order-observation
  (let [incq (q/local-queue)
	[a l] (atom-logger)
        [oa o] (test-observer)
      root (-> (graph)
	       (each (out inc incq))
	       zip/root
	       (observer-rewrite o))]
    (run-sync root [1 2 "fuck" 3])
    (is (= [2 3 4] (sort (wait-for-complete-results incq 5))))
    (is (= [1]
           (get @oa ["lambda" "java.lang.ClassCastException"]) ))))

(deftest priority-in-test
  (let [{:keys [in offer queue f]} (priority-in {:f identity} 5)]
    (offer 10)
    (offer {:item 2 :priority 10})
    (offer {:item 3 :priority 1})
    (is (= 2 (f (in))))
    (is (= 10 (:item (in))))
    (is (= 3 (f (in))))))