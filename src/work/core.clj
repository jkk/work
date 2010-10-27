(ns work.core
  (:refer-clojure :exclude [peek])
  (:import (java.util.concurrent
            Executors ExecutorService TimeUnit
            LinkedBlockingQueue ConcurrentHashMap))
  (:import clojure.lang.RT)
  (:use clj-serializer.core)
  (:require [clj-json [core :as json]]))

(defn from-var [#^Var fn-var]
  (let [m (meta fn-var)]
    [(str (:ns m)) (str (:name m))]))

(defn to-var [#^String ns-name #^String fn-name]
  (let [root (.replace 
              (.replace ns-name "-", "_")
              "." "/")]
    (do 
      (try (RT/load root)
           (catch Exception _ _))
      (.deref (RT/var ns-name, fn-name)))))

(defn- recieve* [msg]
  (let [[[ns-name fn-name] & args] msg]
    (cons (to-var ns-name fn-name) args)))

(defn recieve-clj [msg]
  (recieve* (deserialize (.getBytes msg) (Object.))))

(defn recieve-json [msg]
  (recieve* (json/parse-string msg)))

(def clj-worker (comp eval recieve-clj))

(def json-worker (comp eval recieve-json))

(defn send-clj [fn-var & args]
  (String. (serialize (cons (from-var fn-var) args))))

(defn send-json [fn-var & args]
  (json/generate-string (cons (from-var fn-var) args)))

(defn available-processors []
  (.availableProcessors (Runtime/getRuntime)))

(defn retry [retries f & args]
  "Retries applying f to args based on the number of retries.
  catches generic Exception, so if you want to do things with other exceptions, you must do so in the client code inside f."
  (try (apply f args)
       (catch java.lang.Exception _
         (if (> retries 0)
           (apply retry (- retries 1) f args)
           {:fail 1}))))

;;TODO: try within a retry loop?
(defn try-job [f]
  #(try (f)
        (catch Exception e
          (.printStackTrace e))))

(defn schedule-work
  "schedules work. cron for clojure fns. Schedule a single fn with a pool to run every n seconds,
  where n is specified by the rate arg, or supply a vector of fn-rate tuples to schedule a bunch of fns at once."
  ([pool f rate]
     (.scheduleAtFixedRate pool (try-job f) 0 rate TimeUnit/SECONDS))
  ([jobs]
     (let [pool (Executors/newSingleThreadScheduledExecutor)] 
       (doall (for [[f rate] jobs]
                (schedule-work pool f rate))))))

(defn- work*
  [fns threads]
  (let [pool (Executors/newFixedThreadPool threads)]
    (.invokeAll pool fns)))

(defn work
  "takes a seq of fns executes them in parallel on n threads, blocking until all work is done."
  [fns threads]
  (map #(.get %) (work* fns threads)))

(defn map-work
  "like clojure's map or pmap, but takes a number of threads, executes eagerly, and blocks."
  [f xs threads]
  (work (doall (map (fn [x] #(f x)) xs)) threads))

(defn filter-work
  [f xs threads]
  (filter identity
          (map-work
           (fn [x]
             (if (f x)
               x
               nil))
           xs threads)))

(defn do-work
  "like clojure's dorun, for side effects only, but takes a number of threads."
  [#^java.lang.Runnable f xs threads]
  (let [pool (Executors/newFixedThreadPool threads)
        _ (doall (map
                  (fn [x]
                    (let [#^java.lang.Runnable fx #(f x)]
                      (.submit pool fx)))
                  xs))]
	pool))

(defn local-queue
  ([]
     (LinkedBlockingQueue.))
  ([xs]
     (LinkedBlockingQueue. xs)))

(defn offer [q v] (.offer q v))

(defn offer-all [q vs]
  (doseq [v vs]
    (offer q v)))

(defn offer-unique
  [q v]
  (if (not (.contains q v))
    (.offer q v)))

(defn offer-all-unique [q vs]
  (doseq [v vs]
    (offer-unique q v)))

(defn peek [q] (.peek q))
(defn poll [q] (.poll q))
(defn size [q] (.size q))

;;TODO; unable to shutdown pool. seems recursive fns are not responding to interrupt. http://download.oracle.com/javase/tutorial/essential/concurrency/interrupt.html
;;TODO: use another thread to check futures and make sure workers don't fail, don't hang, and call for work within their time limit?
(defn queue-work
  "schedule-work one worker function f per thread.
  f can either be a fn that is directly applied to each task (all workers use the same fn) or
  f builds and evals a worker from a fn & args passed over the queue (each worker executes an arbitrary fn.)
  Examples of the latter are clj-worker and json-wroker.

  Each worker fn polls the work queue via get-work fn, applies a fn to each dequeued item, puts the result with
  put-done and recursively checks for more work.  If it doesn't find new work, it waits until checking for more work.

  The workers can run in asynchronous mode, where the put-done function is passed to the worker function f,
  and f is responsible for ensuring that put-done is appropriately called.
  Valid values for mode are :sync or :async.  If a mode is not specified, queue-work defaults to :sync."
  ([f get-work put-done threads]
     (queue-work f get-work put-done threads :sync))
  ([f get-work put-done threads mode]
     (let [put-done (if (fn? put-done)
                      put-done
                      (fn [k & args]
                        (apply (put-done k) args)))
           pool (Executors/newFixedThreadPool threads)
           fns (repeat threads
                       (fn [] (do
                                (if-let [task (get-work)]
                                  (if (= :async mode)
                                    (f task put-done)
                                    (put-done (f task)))
                                  (Thread/sleep 5000))
                                (recur))))
           futures (doall (map #(.submit pool %) fns))]
       pool)))

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
  [#^ExecutorService pool]
  (do (.shutdown pool)  ;; Disable new tasks from being submitted
      (try ;; Wait a while for existing tasks to terminate
        (if (not (.awaitTermination pool 60 TimeUnit/SECONDS))
          (.shutdownNow pool) ; // Cancel currently executing tasks
          ;;wait a while for tasks to respond to being cancelled
          (if (not (.awaitTermination pool 60 TimeUnit/SECONDS))
            (println "Pool did not terminate" *err*)))
        (catch InterruptedException _ _
               (do
                 ;;(Re-)Cancel if current thread also interrupted
                 (.shutdownNow pool)
                 ;; Preserve interrupt status
                 (.interrupt (Thread/currentThread)))))))
