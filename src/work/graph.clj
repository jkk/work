(ns work.graph
  (:require
   [clojure.contrib.logging :as log]
   [clojure.zip :as zip]
   [work.core :as work]
   [clojure.contrib.zip-filter :as zf]
   [work.queue :as workq])
  (:use    [plumbing.core ]))

(defprotocol Inbox
  (receive-message [this msg])
  (poll-message [this]))

(defprotocol Outbox
  (broadcast [this x])
  (add-listener [this listener]))

(defrecord Vertex [f inbox outbox])
(defrecord Edge [when convert-task to])

(defn- mk-edge
  [to &
   {:keys [when, convert-task]
    :or {when (constantly true)
	 convert-task (fn [x] [x])}}]
  (Edge. when convert-task to))

(defn drain-to-vertex
  [vertex xs]
  (doseq [x xs] (receive-message (:inbox vertex) x))
  vertex)

(defrecord  InboxQueue [q]
  Inbox
  (receive-message [this msg] (workq/offer q msg))
  (poll-message [this] (workq/poll q)))

(defrecord  DefaultOutbox [out-edges]
  Outbox
  (broadcast [this x]
	     (doseq [{:keys [when,convert-task,to]} out-edges
		     :when (when x)
		     task (convert-task x)]
	       (cond
		(fn? to) (to task)
		(instance? Vertex to) (receive-message (:inbox to) task))))
  (add-listener [this listener] (update-in this [:out-edges] conj listener)))

(defrecord  TerminalOutbox [out]
  Outbox
  (broadcast [this x]
     (workq/offer out x)))

(defn inbox [] (InboxQueue. (workq/local-queue)))
(defn outbox [] (TerminalOutbox. (workq/local-queue)))

(defn exec-vertex
  [{:keys [f,inbox,outbox,sleep-time,exec,make-tasks]
    :or {sleep-time 10
	 exec work/sync}
    :as vertex}]
  ((work/work (fn []
		{:f f
		 :in #(poll-message inbox)
		 :out #(broadcast outbox %)
		 :exec exec})
	      (work/sleeper sleep-time)))
  vertex)

(defn run-vertex
  "launch vertex return vertex with :pool field"
  [{:keys [f,inbox,outbox,threads,sleep-time,exec,make-tasks]
    :or {threads (work/available-processors)
	 sleep-time 10
	 exec work/sync}
    :as vertex}]
  (when (instance? Vertex vertex)
    (let [work-producer
	  (work/work (fn []
		  {:f f
		   :in #(poll-message inbox)
		   :out #(broadcast outbox %)
		   :exec exec})
		(work/sleeper sleep-time))]
	  (assoc vertex
	    :pool (future (work/queue-work
			   work-producer
			   threads))))))

(defn node
  "make a node. only required argument is the fn f at the vertex.
   you can optionally pass in inbox, process, id, or outbox

   Defaults
   --------
   id: gensymd 
   inbox: InboxQueue with new empty queue
   outbox: DefaultOutbox no neighbors
   make-tasks: Make tasks for all children from fn output. Default
   to just output of node function"
  [f  &
   {:keys [id inbox outbox make-tasks]
    :or {inbox (InboxQueue. (workq/local-queue))
         outbox (DefaultOutbox. [])
	 make-tasks (fn [x] [x])
         id (gensym)}
    :as opts}]
  (-> f
      (Vertex. inbox outbox)
      (merge opts)
      (assoc :id id :make-tasks make-tasks)))

(defn terminal-node
  "make a terminal outbox node. same arguments as node
   except that you can pass an :out argument for the queue
   you want the vertex to drain to"
  [& {:keys [f, out] :as opts
      :or {f identity
	   out (workq/local-queue)}}]
  (apply node f
         :outbox (TerminalOutbox. out)
         (apply concat (seq opts))))

(defn root-node []
  (node identity :id :root))

(defn graph-zip
  "make a zipper out of a graph. each zipper location is either
   a graph node (Vertex, fn, or keyword) or a graph edge. So to move from
   a node to its parent you zip/up twice for instance. "
  [root]
  (zip/zipper
   ;; branch?
   (fn [x]
     (or (instance? Edge x)
	 (and (instance? Vertex x) (->> x :outbox (instance? DefaultOutbox)))))
   ;; children
   (fn [x]
     (cond
        (instance? Edge x) [(:to x)]
	(instance? Vertex x) (-> x :outbox :out-edges)
	:default (throw (RuntimeException. (format "Can't take children of %s" (pr-str x))))))
   ;; make-node
   (fn [x cs]
     (cond
      (instance? Edge x)
        (do
	  (assert (and (= (count cs) 1)))
	  (assoc x :to (first cs)))
      (instance? Vertex x)
        (do
	  (every? (partial instance? Edge) cs)
	  (assoc-in x [:outbox :out-edges] cs))
	:default (throw (RuntimeException.
			 (format "Can't make node from %s and %s" x cs)))))
   ; root 
   root))

(defn- add-edge-internal
  [src out-edge]
  (update-in src [:outbox :out-edges] conj out-edge))

(defn add-edge
  "Add edge to current broadcast node in graph-loc. Does
   not change zipper location.

   trg: Either Vertex, :recur or fn

   message is passd to Vertex 
   fn is executed on messgage
   :recur message sent back to vertex
   
   Edge options
   ===============
   :when predicate returning when message is sent to edge
   :convert-task a fn that takes an input task and returns a seq
   of tasks to put in inbox. Default to singleton of input task
   (i.e., no convertion)"
  [graph-loc trg & opts]
  (zip/edit graph-loc add-edge-internal (apply mk-edge trg opts)))

(defn add-edge->
  "same as add-edge but also moves zipper cursor to new trg"
  [graph-loc trg & opts]
  (assert (instance? Vertex (zip/node graph-loc)))
  (-> graph-loc
      (zip/edit add-edge-internal (apply mk-edge trg opts))
      zip/down
      zip/rightmost
      zip/down))

(defn- all-vertices [root]
  (for [loc  (zf/descendants (graph-zip root))
	:when (and loc (->> loc zip/node (instance? Vertex)))]
    (zip/node loc)))

(defn run-graph
  "launches in DFS order the vertex processs and returns a map from
   terminal node ids (possibly gensymd) to their out-queues"
  [graph-loc]
  (let [root (zip/root graph-loc)
	update (fn [loc]
		 (if (instance? Vertex (zip/node loc))
		   (zip/edit loc run-vertex)
		   loc))]
    (loop [loc (graph-zip root)]
      (if (zip/end? loc)
	(zip/root loc)
	(recur (-> loc update zip/next))))))

(defn terminal-queues [root]
  (reduce
   (fn [res v]
     (assoc res (:id v) (-> v :outbox :out)))
   {}
   (filter #(instance? TerminalOutbox (:outbox %))
          (all-vertices root))))

(defn kill-graph [root & to-exclude]
  (let [to-exclude (into #{} to-exclude)]
    (doseq [n (-> root all-vertices)
	    :when (not (to-exclude (:id n)))]
      (-?> n :pool deref work/two-phase-shutdown))))