(ns work.queue
  (:refer-clojure :exclude [peek])
  (:use plumbing.core plumbing.serialize)        
  (:import (java.util.concurrent LinkedBlockingQueue
                                 PriorityBlockingQueue)))

(defn poll [^java.util.Queue q] (.poll q))

(defn offer [^java.util.Queue q] (.offer q))

(defn peek [^java.util.Queue q] (.peek q))

(defn contains [^java.util.Queue q x] (.contains q x))

(defn offer [^java.util.Queue q x] (.offer q x))

(defn local-queue
  "return LinkedBlockingQueue implementation
   of Queue protocol."
  ([]
     (LinkedBlockingQueue.))
  ([^java.util.Collection xs]
     (LinkedBlockingQueue. xs)))

(defn priority-queue
  ([]
     (PriorityBlockingQueue.))
  ([^java.util.Collection xs]
     (PriorityBlockingQueue. xs))
  ([init-size ordering]
     (PriorityBlockingQueue. init-size (comparator ordering))))

(defn priority [x y]
  (> (:priority x)
     (:priority y)))

(defn with-adapter
  [in-adapt out-adapt queue]
  (reify
      java.util.Queue
      (poll [q] (out-adapt (poll queue)))
      (offer [q x] (offer queue (in-adapt x)))
      (contains [q x] (contains queue x))
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
  (locking q
	 (if (not (.contains q v))
	   (offer q v))))

(defn offer-all-unique [q vs]
  (doseq [v vs]
    (offer-unique q v)))