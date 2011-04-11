(ns work.queue
  (:refer-clojure :exclude [peek])
  (:use plumbing.core
        plumbing.serialize
        [plumbing.error :only [with-ex logger]])
  (:import (java.util.concurrent LinkedBlockingQueue
                                 PriorityBlockingQueue)))

(defprotocol Queue
  (poll [q] "poll")
  (alive? [this] "is the queue alive?")
  (peek [q] "see top elem without return")
  (offer [q x] "offer"))

(extend-protocol Queue
  LinkedBlockingQueue
  (poll [this] (.poll this))
  (offer [this x]
         (if-let [r (.offer this x)]
           r
           (throw (Exception. "Queue offer failed"))))
  (peek [this] (.peek this))
  (alive? [this] (nil? (.peek this))))

(defn local-queue
  "return LinkedBlockingQueue implementation
   of Queue protocol."
  ([]
     (LinkedBlockingQueue.))
  ([^java.util.Collection xs]
     (LinkedBlockingQueue. xs)))

(extend-protocol Queue
  PriorityBlockingQueue
  (poll [this] (.poll this))
  (offer [this x]
         (if-let [r (.offer this x)]
           r
           (throw (Exception. "Queue offer failed"))))
  (peek [this] (.peek this))
  (alive? [this] (nil? (.peek this))))

(defn priority-queue
  ([]
     (PriorityBlockingQueue.))
  ([^java.util.Collection xs]
     (PriorityBlockingQueue. xs))
  ([init-size ordering]
     (PriorityBlockingQueue. init-size (comparator ordering))))

(defn by-key [f k]
  (fn [x y]
    (f (x k) (y k))))

(defn with-adapter
  [in-adapt out-adapt queue]
  (reify
         Queue
	 (poll [q] (out-adapt (poll queue)))
	 (offer [q x] (offer queue (in-adapt x)))
	 (peek [q] (out-adapt (peek queue)))

	 clojure.lang.Seqable
	 (seq [this] (map out-adapt (seq queue)))

	 clojure.lang.Counted
	 (count [this] (count queue))))

(defn with-serialize
  "decorates queue with serializing input and output to and from bytes."
  ([serialize-impl queue]
     (with-adapter
       (partial serialize serialize-impl)
       (partial deserialize serialize-impl)
       queue))
  ([q] (with-serialize (string-serializer) q)))

(defn offer-all [q vs]
  (doseq [v vs]
    (offer q v)))

(defn offer-unique
  "assumes q is a Queue and Seqable"
  [q v]
  (if (not (find-first (partial = v) q))
    (offer q v)))

(defn offer-all-unique [q vs]
  (doseq [v vs]
    (offer-unique q v)))

(defn priority-process [f refill cb]
  (let [q (priority-queue
           200
           (by-key > :priority))
        q-refill (fn [] (when-let [ms (refill)]
                          (offer-all q ms)))
        get-job (partial with-ex (logger)
                         (fn []
                           (if-let [m (poll q)]
                             (:job m)
                             (do (q-refill)
                                 (:job (poll q))))))
        start (fn []
                (if-let [m (get-job)]
                  (do (f m)
                      (cb m)))
                (recur))
        put-job (fn [j pri]
                  (offer q {:job j
                            :priority pri}))]
    [start put-job]))