(ns work.graph
  (:use    [plumbing.error :only [-?> with-ex logger]]
	   [plumbing.core :only [?>>]]
	   [plumbing.serialize :only [gen-id]])
  (:require
   [clojure.contrib.logging :as log]
   [clojure.zip :as zip]
   [work.core :as work]
   [clojure.contrib.zip-filter :as zf]
   [work.queue :as queue])
  (:import [java.util.concurrent Executors]))

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

(defn- add-child
  [parent child]
  (update-in parent [:children] conj child))

(defn child
  [parent-loc child]
  (zip/edit parent-loc add-child child))

(defmacro subgraph [parent-loc & subs]
  `(child
    ~parent-loc
    (-> (graph)
	~@subs
	zip/root)))

(defn each [parent-loc & args]
  (child parent-loc
	 (apply node args)))

(defn multimap [parent-loc f & args]
  (child parent-loc
	 (apply node f :multimap true args)))

(def >> (comp zip/leftmost zip/down))

(defn- all-vertices [root]
  (for [loc  (zf/descendants (graph-zip root))
	:when (and loc (->> loc zip/node))]
    (zip/node loc)))

(defn comp-rewrite
  [{:keys [f children multimap when] :as vertex}]		   
  {:f (fn [x]
	(swank.core/break)
	(if (or (not when) (when x))
	  (let [fx (f x)]
	    (doseq [cx (if multimap fx [fx])
		    child children
		    :let [childf (-> child comp-rewrite :f)]]
	      (childf cx))
	    fx)))
   :id "all"})

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
		      :when (cwhen x)]
		(queue/offer in x)))]
    (assoc vertex
      :out out
      :children children)))

(defn out [f q]
  (fn [& args]
    (queue/offer q (apply f args))))

(defn update-nodes [f root]
  (let [update (fn [l] (zip/edit l f))]
    (loop [loc (graph-zip root)]
	(if (zip/end? loc)
	  (zip/root loc)
	  (recur (-> loc update zip/next))))))

(defn add-pool
  [{:keys [threads]
    :or {threads (work/available-processors)}
    :as vertex}]  
  (let [pool (work/queue-work
	      (constantly vertex) threads)]
    (update-in vertex [:shutdown] conj (fn [] (work/two-phase-shutdown pool)))))

(defn fifo-in [root]
  (let [in (queue/local-queue)]
    (assoc root
      :queue in
      :in #(queue/poll in))))

(defn priority-fn [f]
  (fn [{:keys [item callback]}]
    (if (not callback)
      (f item)
      (let [result (f item)]
	(callback item)
	result))))

(defn priority-in [{:keys [f] :as root}]
  (let [in (queue/priority-queue
	    200
	    queue/priority)]
    (assoc root
      :queue in
      :offer #(queue/offer-unique in %)
      :f (priority-fn f)
      :in #(queue/poll in))))

(defn refill [refill {:keys [queue] :as root}]
  (when (empty? queue)
    (with-ex (logger) queue/offer-all queue (remove nil? (refill))))
  root)

(defn schedule-refill [refill-fn freq {:keys [queue] :as root}]
  (let [pool (work/schedule-work #(refill refill-fn root) freq)]
    (update-in root [:shutdown] conj (fn [] (work/two-phase-shutdown pool)))))

(defn observer-rewrite [observer root]
  (update-nodes
    (fn [v] (assoc v :f (observer v)))
    root))

(defn graph-rewrite [rewrites root]
  (reduce
     (fn [root rewrite] (rewrite root))
     root
     rewrites))

(defn run-sync [graph-loc data & rewrites]
  (let [mono (->>  graph-loc
		   zip/root
		   (graph-rewrite rewrites)
		   comp-rewrite
		   :f)]
    (doseq [x data] (mono x))))

(defn run-pool
  [graph-loc & rewrites]
  (->> graph-loc
       zip/root
       (graph-rewrite rewrites)
       queue-rewrite
       fifo-in
       (update-nodes add-pool)))

(defn kill-graph [root]
  (doseq [n (-> root all-vertices)
	  f (:shutdown n)]
    (with-ex (logger) f)))
