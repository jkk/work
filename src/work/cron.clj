(ns work.cron
  (:use
   [plumbing.error :only [with-ex logger]])
  (:import
   [java.util.concurrent Executors ExecutorService]
   [java.util Timer TimerTask]))

(defn cron
  "Make a cron service backed by a cached threadpool, with which jobs can be scheduled or canceled."
  []
  {:executor (Executors/newCachedThreadPool)
   :timer    (Timer. "Cron")
   :jobs     (atom {})})

(defn- timer-task [f]
  (proxy [TimerTask] [] (run [] (f))))

(defn schedule-job! [{:keys [^ExecutorService executor ^Timer timer jobs] :as cron} job-id f period-ms]
  (locking timer
    (assert (not (contains? @jobs job-id)))
    (let [tt ^TimerTask (timer-task #(.submit executor ^Runnable (partial with-ex (logger) f)))]
      (.schedule timer tt (long 0) (long period-ms))
      (swap! jobs assoc job-id tt))))

(defn list-jobs [cron] (keys @(:jobs cron)))

(defn cancel-job! [{:keys [^Timer timer jobs]} job-id]
  (locking timer
    (let [tt ^TimerTask (get @jobs job-id)]
      (assert tt)
      (.cancel tt)
      (swap! jobs dissoc job-id))))

(defn cancel-all! [c]
  (doseq [j (list-jobs c)] (cancel-job! c j)))
