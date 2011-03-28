(ns work.graph
  (:require
   [clojure.contrib.logging :as log]
   [clojure.zip :as zip]
   [work.core :as work]
   [clojure.contrib.zip-filter :as zf]
   [work.queue :as workq])
  (:use    [plumbing.core ]))

(defn node
  [f  &
   {:keys [id]
    :or {id (gensym)}
    :as opts}]
  (-> {:f f}
      (merge opts)
      (assoc :id id)))

(defn multimap [f & args]
  (apply node f :multimap true args))

(defn root-node []
  (node identity :id :root))

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

(defn- add-child
  [parent child]
  (update-in parent [:children] conj child))

(defn child
  [parent-loc child]
  (zip/edit parent-loc add-child child))

(defn child->
  [parent-loc child]
  (-> parent-loc
      (zip/edit add-child child)
      zip/down
      zip/rightmost))

(defn- all-vertices [root]
  (for [loc  (zf/descendants (graph-zip root))
	:when (and loc (->> loc zip/node))]
    (zip/node loc)))

(defn run-vertex
  "launch vertex return vertex with :pool field"
  [{:keys [f,in,out,threads,sleep-time,exec,multimap]
    :or {threads (work/available-processors)
	 sleep-time 10
	 exec work/sync
	 multimap list}
    :as vertex}]
  (let [work-producer
	(work/work (fn []
		     {:f f
		      :in #(workq/poll in)
		      :out (fn [x]
			     (doall (map #(workq/offer out %)
					 (multimap x))))
		      :exec exec})
		   (work/sleeper sleep-time))]
    (assoc vertex
      :pool (future (work/queue-work
		     work-producer
		     threads)))))

(defn run-graph
  "launches in DFS order the vertex processs and returns a map from
   terminal node ids (possibly gensymd) to their out-queues"
  [graph-loc]
  (let [root (zip/root graph-loc)
	update (fn [loc]
		 (zip/edit loc run-vertex))]
    (loop [loc (graph-zip root)]
      (if (zip/end? loc)
	(zip/root loc)
	(recur (-> loc update zip/next))))))

(defn graph-comp [{:keys [f children multimap when]}]
  (if (not children)
    (if (not when) f
	(fn [& args]
	  (if (apply when args)
	    (apply f args))))
    (fn [& args]
      (let [fx (apply f args)]
	(doall (map (fn [child]
		      (if (not multimap)
			((graph-comp child) fx)
			(doall (map (graph-comp child) fx))))
		    children))))))

(defn out [f q]
  (fn [& args]
    (workq/offer q (apply f args))))

(defn mono-run [graph-loc data]
  (let [f (graph-comp (zip/root graph-loc))]
       (doall (map f data))))

(defn kill-graph [root]
  (doseq [n (-> root all-vertices)]
    (-?> n :pool deref work/two-phase-shutdown)))