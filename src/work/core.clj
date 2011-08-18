(ns work.core
  (:refer-clojure :exclude [peek sync])
  (:require [clj-json [core :as json]]
	    [work.queue :as workq]
            [clojure.contrib.logging :as log])
  (:use work.queue
	[store.core :only [bucket-seq bucket-merge-to! bucket-merge bucket hashmap-bucket with-merge]]
        [clojure.contrib.def :only [defvar]]
        [plumbing.core :only [with-accumulator]]
        [plumbing.error :only [with-ex logger]])
  (:import (java.util.concurrent
            Executors ExecutorService TimeUnit
	    CountDownLatch
            LinkedBlockingQueue)))

(defn available-processors []
  (.availableProcessors (Runtime/getRuntime)))

(defn schedule-work
  "schedules work. cron for clojure fns. Schedule a single fn with a pool to run every n seconds,
  where n is specified by the rate arg, or supply a vector of fn-rate tuples to schedule a bunch of fns at once."
  [f rate]
  (doto (Executors/newSingleThreadScheduledExecutor)
    (.scheduleAtFixedRate 
         (partial with-ex (logger) f)
	 (long 0)
	 (long rate)
	 TimeUnit/SECONDS)))

(defn shutdown
  "Initiates an orderly shutdown in which previously submitted tasks are executed, but no new tasks will be accepted. Invocation has no additional effect if already shut down."
  [executor]
  (do (.shutdown executor) executor))

(defn shutdown-now [executor]
  "Attempts to stop all actively executing tasks, halts the processing of waiting tasks, and returns a list of the tasks that were awaiting execution.

  There are no guarantees beyond best-effort attempts to stop processing actively executing tasks. For example, typical implementations will cancel via Thread.interrupt(), so if any tasks mask or fail  to respond to interrupts, they may never terminate."
  (do (.shutdownNow executor) executor))

(defn two-phase-shutdown
  "Shuts down an ExecutorService in two phases.
  Call shutdown to reject incoming tasks.
  Calling shutdownNow, if necessary, to cancel any lingering tasks.
  From: http://download-llnw.oracle.com/javase/6/docs/api/java/util/concurrent/ExecutorService.html"
  [^ExecutorService pool]
  (do (.shutdown pool)  ;; Disable new tasks from being submitted
      (with-ex ;; Wait a while for existing tasks to terminate
	(fn [e _ _]
	  (when (instance? InterruptedException e)
	    ;;(Re-)Cancel if current thread also interrupted
	    (.shutdownNow pool)
	    ;; Preserve interrupt status
	    (.interrupt (Thread/currentThread))))	
        #(if (not (.awaitTermination pool 60 TimeUnit/SECONDS))
	  (.shutdownNow pool) ; // Cancel currently executing tasks
          ;;wait a while for tasks to respond to being cancelled
          (if (not (.awaitTermination pool 60 TimeUnit/SECONDS))
            (println "Pool did not terminate" *err*))))))

(defn async [f task out]
  (f task out))

(defn sync [f task out]
  (out (f task)))

(defn sleeper-exp-strategy
  [ready?  &
   {:keys [start alpha]
    :or {start 10 alpha 2.0}}]	 
   (loop [sleep-time start]
     (if-let [r (ready?)]
       r
       (do (when (Thread/interrupted) (throw (InterruptedException.)))
	   (Thread/sleep sleep-time)
	   (recur (* alpha sleep-time))))))

(defn exec-work
  [schedule-work]
  (let [work (schedule-work)]
    (when (not= work :done)
      (let [{:keys [f in out exec sleep-time clean-up]
	     :or {exec sync
		  sleep-time 200
		  clean-up (constantly nil)	 
		  out identity}} work
		  task (in)]
	(if (nil? task)
	  (Thread/sleep sleep-time)
	  (try
	    (exec f task out)
	    (catch Exception e
	      (log/error
	        "Top-level work.core exception in exec-work!")
	      (.printStackTrace e))
	    (finally (clean-up task))))
	(recur schedule-work)))))

(defn submit-to [^ExecutorService pool schedule-work]
  (.submit pool
	   (cast Runnable
	         #(exec-work
		   (fn []
		    (if (.isShutdown pool)
		      :done
		      (schedule-work)))))))

(defn queue-work [schedule-work num-workers]
  (let [pool (Executors/newFixedThreadPool (int num-workers))]
    (dotimes [_ num-workers] (submit-to pool schedule-work))
    pool))

(defn do-work
  ([f num-workers tasks]
     (do-work (repeat num-workers f) tasks))
  ([workers tasks]
      (when-not (empty? tasks)
	(let [pool (Executors/newFixedThreadPool (count workers))
	      tasks (seq tasks)
	      in (workq/local-queue tasks)
	      latch (CountDownLatch. (count tasks))
	      workers (map (fn [f]
			     {:f f
			      :in #(workq/poll in)
			      :clean-up (fn [& _] (.countDown latch))})
			   workers)]		  
	  (doseq [worker workers]
	    (submit-to pool #(if (empty? in) :done worker)))
	  (.await latch)
	  (future (two-phase-shutdown pool))))))

(defn map-work
  ([f num-workers tasks]
     (map-work (repeat num-workers f) tasks))
  ([workers tasks]
     (let [pool (Executors/newFixedThreadPool (count workers))
	   latch (CountDownLatch. (count tasks))
	   tasks (seq tasks)
	   out-queue (workq/local-queue)
	   in-queue (workq/local-queue tasks)
	   workers (map (fn [f] {:f f
				:in #(workq/poll in-queue)
				:out (partial workq/offer out-queue)
				:clean-up (fn [& _] (.countDown latch))})
		       workers)]
       (doseq [worker workers]
	 (submit-to pool #(if (empty? in-queue) :done  worker)))
       (take-while (fn [x] (not (= :eof x)))
		   (repeatedly
		    #(sleeper-exp-strategy
		      (fn []
			(if (and (.isEmpty out-queue) (zero? (.getCount latch)))
			  (do (shutdown-now pool)
			      :eof)			    
			  (workq/poll out-queue)))))))))

(defn map-reduce [map-fn reduce-fn num-workers input]
  (let [pool (Executors/newFixedThreadPool (int num-workers))
	get-bucket #(bucket {:type :mem
                             :merge (fn [_ accum new] (reduce-fn accum new))})
	res (get-bucket)
	in-queue (workq/local-queue input)
	latch (CountDownLatch. (int  num-workers))
	defaults {:f map-fn :in #(workq/poll in-queue)}]
    (dotimes [_ num-workers]
      (submit-to pool
	 (let [b (get-bucket)]
	   #(if (empty? in-queue)
	     (do (try
		   (bucket-merge-to! b res)
		   (finally (.countDown latch)))
		 :done)
	     (assoc defaults :out
	       (fn [kvs]
		 (doseq [[k v] kvs]
		   (bucket-merge b k v))))))))       
    (.await latch)
    (shutdown-now pool)
    res))

(defn mapchunk-reduce
  [map-fn reduce-fn num-workers chunk-size input]
  (->> input
       (partition-all chunk-size)
       (map-reduce
	 (fn [input] (mapcat map-fn input))
	 reduce-fn
	 num-workers)))

(defn keyed-producer-consumer
  "when you have data associated with a given key coming in. ensure that you accumulate
   data with a merge-fn and keyed data is worked on and at most  one worker is processing a given
   keys data at a given time

   returns [put-work, get-work, done-work]. A consumer work gets a [key data] pair from
   get-work and when done pings wiht (done-work). The producer puts [k data] with put-work.
   The passed in merge-fn is used to add keyed data."
  [merge-fn]
  (let [tokens  (java.util.concurrent.ConcurrentHashMap.)
	data (java.util.concurrent.ConcurrentHashMap.)
	bucket (with-merge (hashmap-bucket data) merge-fn)
	q (java.util.concurrent.ConcurrentLinkedQueue.)
	put-work (fn [k v]
		   (bucket-merge bucket k v)
		   (.add q k))
	get-work (fn []
		   (when-let [k (.poll q)]
		     (let [tok (.putIfAbsent tokens k true)]
		       ;; no pevious value
		       (if (nil? tok)
			 [k (.remove  data k)]
			 ;; put k back in queue,
			 ;; try another key			 
			 (do (.add q k)
			     (recur))))))
	done-work (fn [k] (.remove tokens k))]
    [put-work get-work done-work]))