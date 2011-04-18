(ns work.graph
  (:require
   [clojure.contrib.logging :as log]
   [clojure.zip :as zip]
   [work.core :as work]
   [clojure.contrib.zip-filter :as zf]
   [work.queue :as workq])
  (:use    [plumbing.error :only [-?>]])
  (:import [java.util.concurrent Executors])
  (:use    [plumbing.serialize :only [gen-id]]))

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

(defn pred-f [f when]
  (if (not when) f
      (fn [& args]
	(if (apply when args)
	  (apply f args)))))

(defn multi-f [f multimap v]
  (if (not multimap)
    (f v)
    (doseq [x v]
      (f x))))

(defn graph-comp [{:keys [f children multimap when]
		   :as vertex}
		  & [observer]]
  (let [obsf (if observer (observer vertex) f)]
    (if (not children)
      (pred-f obsf when)
      (pred-f (let [childfs (doall (map #(graph-comp % observer) children))]
		(fn [& args]
		  (let [fx (apply obsf args)]
		    (doseq [childf childfs]
		      (multi-f childf multimap fx)))))
	       when))))

(defn out [f q]
  (fn [& args]
    (workq/offer q (apply f args))))

(defn run-sync [graph-loc data & [obs]]
  (let [f (graph-comp (zip/root graph-loc) obs)
	mono (if (not obs)
	       f (obs {:f f :id "all"}))]
    (doseq [x data]
      (mono x))))

(defn run-vertex
  [{:keys [threads]
    :or {threads (work/available-processors)}
    :as vertex}]
  (assoc vertex
    :pool (let [pool (Executors/newFixedThreadPool (int threads))]
	    (dotimes [_ threads]
	      (work/submit-to pool (constantly vertex)))
	    pool)))

(defn run-graph
  [graph-loc]
  (let [root (zip/root graph-loc)
	update (fn [loc]
		 (zip/edit loc run-vertex))]
    (loop [loc (graph-zip root)]
      (if (zip/end? loc)
	(zip/root loc)
	(recur (-> loc update zip/next))))))

(defn run-pool [graph-loc threads in & [obs]]
  (let [f (graph-comp (zip/root graph-loc) obs)
	mono (if (not obs)
	       f (obs {:f f :id "all"}))
	rewritten-graph (-> (graph)
			    (each mono
				  :in #(workq/poll in)
				  :out (fn [& args])
				  :threads threads))]
    (run-graph rewritten-graph)))

(defn kill-graph [root]
  (doseq [n (-> root all-vertices)]
    (-?> n :pool deref work/two-phase-shutdown)))