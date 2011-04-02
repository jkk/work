(ns work.aggregators
  (:import java.util.concurrent.Executors)
  (:use [plumbing.core]
	[clojure.contrib.map-utils :only [deep-merge-with]]
	store.api
	[work.core :only [available-processors seq-work do-work
			  map-work schedule-work shutdown-now]]
	[work.queue :only [local-queue]]))

(defprotocol IAgg
  (agg [this v][this k v])
  (agg-inc [this][this v]))

(defn +maps [ms]
  (apply
   deep-merge-with
   + (remove nil? ms)))

(defn agg-bucket [bucket merge done]
  (let [counter (java.util.concurrent.atomic.AtomicInteger.)
	do-and-check (fn [f]
		       (try (f)
			    (finally
			     (when
				 (<= (.decrementAndGet counter) 0)
 			       (done bucket)))))]
    (reify IAgg
	     (agg [this k v]
			    (do-and-check
			     #(bucket-update
			       bucket k (fn [x] (merge [x v])))))
	     (agg [this v]
			    (do-and-check
			     #(bucket-merge-to!
			       v
			       (with-merge bucket
				 (fn [k & args] (merge args))))))
	     (agg-inc [this v] (.addAndGet counter v))
	     (agg-inc [this] (.incrementAndGet counter)))))

(defn mem-agg [merge]
  (agg-bucket (hashmap-bucket) merge
	      #(into {} (bucket-seq %))))

(defn agg-work
  [f num-threads aggregator tasks]
  (let [tasks (seq tasks)
	pool (Executors/newFixedThreadPool num-threads)]
    (agg-inc aggregator (int (count tasks)))
    (doseq [t tasks :let [work #(f t)]]	       
      (.submit pool ^java.lang.Runnable work))
    (shutdown-now pool)))

(defn map-reduce
  ([map-fn reduce-fn num-threads-or-pool init xs]
     (let [res (atom init)
	   accum-res (fn [t] (swap! res reduce-fn t))]
       (do-work (comp accum-res map-fn) num-threads-or-pool xs)
       @res)))