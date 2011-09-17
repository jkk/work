(ns work.core-test
  (:use clojure.test
	[plumbing.core :only [retry wait-until]]
	[plumbing.serialize :only [send-clj clj-worker
				   send-json json-worker]])
  (:require [work.core :as work]
	    [store.core :as bucket])
  (:require [work.queue :as q])
  (:import [java.util.concurrent Executors]))

(defn wait-for-complete-results
  "Test helper fn waits until the pool finishes processing before returning results."
  [response-q expected-seq-size]
  (wait-until #(= (.size response-q) expected-seq-size) 20)
  (sort (iterator-seq (.iterator response-q))))

(deftest do-work-test
  (let [input-data (range 1 101 1)
        response-q (q/local-queue)]
    (work/do-work #(q/offer response-q (* 10 %))
		  10
		  input-data)
    (is (= (range 10 1010 10)
           (wait-for-complete-results response-q (count input-data))))))

(deftest map-work-test
  (is (= (range 10 1010 10)
	 (sort (work/map-work
		#(* 10 %)
		10
		(range 1 101 1))))))

(deftest map-reduce-test
  (is (=
       {:a 13 :b 4 :c 4 :d 3}
       (into {}
	     (bucket/seq
	      (work/map-reduce
	       frequencies
	       (fnil + 0 0)
	       5
	       [[:a :a :b :b]
		[:c :c :a :a :a]
		[:d :d :d :a :a]
		[:c :c :a :a :a]
		[:b :b :a :a :a]]))))))


(deftest mapchunk-reduce-test
  (is (=
       {:a 13 :b 4 :c 4 :d 3}
       (into {}
	     (bucket/seq
	      (work/mapchunk-reduce
	       frequencies
	       (fnil + 0 0)
	       5
	       2
	       [[:a :a :b :b]
		[:c :c :a :a :a]
		[:d :d :d :a :a]
		[:c :c :a :a :a]
		[:b :b :a :a :a]]))))))

(deftest trivial-map-work-test
  (is (.get (future (doall (work/map-work (fn [x] (do (Thread/sleep x)
						     1)) 200 (range 100))))
            (long 300) java.util.concurrent.TimeUnit/SECONDS)))

(deftest worker-exception-test
  (let [p (Executors/newFixedThreadPool 2)
	c (atom 0)]
    (work/submit-to p
		    (constantly
		      {:in (constantly 42)
		       :f (fn [_]
			    (swap! c inc)
			    (/ 1 0))}))
    (Thread/sleep 100)
    (.shutdownNow p)
    (is (> @c 1))))

