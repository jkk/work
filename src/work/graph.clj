(ns work.graph
  (:use    [plumbing.error :only [-?> with-ex logger assert-keys ex-name cause]]
	   [plumbing.core :only [?>> map-map]]
	   [plumbing.serialize :only [gen-id]]
	   [clojure.zip :only [insert-child]]
	   [store.api :only [store]]
	   [work.message :only [add-subscriber]])
  (:require
   [plumbing.observer :as obs]
   [clojure.contrib.logging :as log]
   [clojure.zip :as zip]
   [work.core :as work]
   [work.message :as message]
   [clojure.contrib.zip-filter :as zf]
   [work.queue :as queue])
  (:import [java.util.concurrent BlockingQueue Executors]))

(defn node
  [f  &
   {:keys [id]
    :or {id (gen-id f)}
    :as opts}]
  (-> {:f f}
      (merge opts)
      (assoc :id id)))

(defn graph-zip
  [root]
  (zip/zipper
   ;; branch?
   :children
   ;; children
   :children
   ;; make-node
   (fn [x cs] (assoc-in x [:children] cs))
   ; root 
   root))

(defn graph []
  (graph-zip
   (node identity :id :root)))

(defn add-child
  [parent child]
  (update-in parent [:children] conj child))

(defn child
  [parent-loc child]
  (zip/edit parent-loc add-child child))

(defn each [parent-loc & args]
  (child parent-loc
	 (apply node args)))

(def >> (comp zip/leftmost zip/down))

(defn last-loc [root]
  (-> root zf/descendants last))

(defn inline-graph
  [parent-loc child-graph-loc]
  (->>
   child-graph-loc
   zip/root
   :children
   (reduce child
	   parent-loc)
   last-loc))

(defmacro subgraph [parent-loc & subs]
  `(child
    ~parent-loc
    (-> (graph)
	~@subs
	zip/root)))

(defn multimap [parent-loc f & args]
  (child parent-loc
	 (apply node f :multimap true args)))

(defn- all-vertices [root]
  (for [loc  (zf/descendants (graph-zip root))
	:when (and loc (->> loc zip/node))]
    (zip/node loc)))

(defn comp-rewrite
  [{:keys [f children multimap when] :as vertex}
   & [threads]]		   
  {:f (fn [x]
	(if (and x (or (not when) (when x)))
	  (let [fx (f x)]
	    (doseq [cx (if multimap fx [fx])
		    child children
		    :let [childf (-> child comp-rewrite :f)]]
	      (childf cx))
	    fx)))
   :id "all"
   :threads threads})

(defn queue-rewrite
  [{:keys [f children multimap when] :as vertex}]
  (let [ins (take (count children) (repeatedly #(queue/local-queue)))
	children (map (fn [child in]
			(assoc (queue-rewrite child) :in #(queue/poll in)))
		      children ins)
	out (fn [input]
	      (doseq [x (if multimap input [input])
		      [c in] (zipmap children ins)
		      :let [cwhen (or (:when c) (constantly true))]
		      :when (and x (cwhen x))]
		(queue/offer in x)))]
    (assoc vertex
      :out out
      :children children)))

(defn out [f q]
  (fn [& args]
    (queue/offer q (apply f args))))

(defn update-nodes [root f]
  (let [update (fn [l] (zip/edit l f))]
    (loop [loc (graph-zip root)]
	(if (zip/end? loc)
	  (zip/root loc)
	  (recur (-> loc update zip/next))))))

(defn update-loc [loc id f]
  (let [id? (fn [l] (= id (:id (zip/node l))))
	update (fn [l] (zip/edit l f))]
    (loop [loc (-> loc zip/root graph-zip)]
      (cond (id? loc)
	    (-> loc update zip/root graph-zip)
	    (zip/end? loc)
	    (-> loc zip/root graph-zip)
	    :else (recur (-> loc zip/next))))))

(defn append-child [root id child-node]
  (update-loc
   root
   id
   #(add-child % (apply node (:f child-node)
			(flatten (seq child-node))))))

(defn add-pool
  [{:keys [threads]
    :or {threads (work/available-processors)}
    :as vertex}]  
  (let [pool (work/queue-work
	      (constantly vertex) threads)]
    (update-in vertex [:shutdown] conj (fn [] (work/two-phase-shutdown pool)))))

(defn fifo-in [root & [capacity]]
  (let [in (if capacity
	     (queue/blocking-queue capacity)
	     (queue/local-queue))]
    (assoc root
      :queue in
      :offer (if capacity
	       #(.put ^BlockingQueue in %)
	       #(queue/offer in %))	       
      :in #(queue/poll in))))

(defn priority-fn [f]
  (fn [{:keys [item callback]}]
    (if (not callback)
      (f item)
      (let [result (f item)]
	(callback item)
	result))))

(defn priority-in [{:keys [f] :as root} priority]
  (assert-keys [:f] root)
  (let [in (queue/priority-queue
	    200
	    queue/priority)]
    (assoc root
      :queue in
      :offer #(queue/offer
	       in
	       (queue/priority-item priority %))
      :f (priority-fn f)
      :in #(queue/poll in))))

(defn refill [{:keys [offer queue] :as root} refill]
  (assert-keys [:offer :queue] root)
  (when (empty? queue)
    (let [o (partial with-ex (logger) offer)]
      (doseq [x (remove nil? (refill))]
	(o x))))
  root)

(defn schedule-refill [root refill-fn freq & [clear-on-refill?]]
  (let [pool (work/schedule-work
	      (fn []
		 (when clear-on-refill?
		   (-> root :queue .clear))
		 (refill root refill-fn)) freq)]
    (update-in root [:shutdown]
	       conj (fn [] (work/two-phase-shutdown pool)))))

(defn graph-rewrite [root rewrites]
  (reduce
     (fn [root rewrite] (rewrite root))
     root
     rewrites))

(defn subscribe
  [{:keys [offer] :as root}
   {:keys [local]}
   subscriber]
  (assert (nil? (:f subscriber)))
  (add-subscriber local (assoc subscriber :f offer))
  root)

(defn publish [root broker parent-id config]
  (let [n (message/publisher broker config)]
    (append-child
     root
     parent-id
     (assoc config :f n))))

(defn run-sync [graph-loc data]
  (let [mono (-> graph-loc comp-rewrite :f)]
    (doseq [x data] (mono x))))

(defn run-pool
  [graph-loc]
  (-> graph-loc
      queue-rewrite
      fifo-in
      (update-nodes add-pool)))

(defn kill-graph [root]
  (doseq [n (-> root all-vertices)
	  f (:shutdown n)]
    (with-ex (logger) f)))

(defn observe-node [observer {:keys [id f] :as node}]
  (assoc node :f (obs/observed-fn observer id :stats f)))

(defn observer-rewrite [root observer]
  (->> (obs/sub-observer observer (obs/gen-key "graph"))
       (partial observe-node)
       (update-nodes root)))