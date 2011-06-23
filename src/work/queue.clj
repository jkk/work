(ns work.queue
  (:refer-clojure :exclude [peek])
  (:import (java.util.concurrent LinkedBlockingQueue
                                 PriorityBlockingQueue))
  (:use [services.core :only [client-wrapper fn-handler start-web]]
	[store.api :only [store]]
	[plumbing.core :only [?> ?>> keywordize-map]]
	[plumbing.error :only [with-ex logger]]
	[plumbing.cache :only [refreshing-resource]]
	[plumbing.accumulators :only [draining-fn]]
	[store.net :only [rest-store-handler]]
	[compojure.core :only [routes]]
        [ring.adapter.jetty :only [run-jetty]]))

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

(defn priority-item [priority item]
  (if (and (map? item) (:item item))
    (merge {:priority priority} item)
    {:item item :priority priority}))

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


(defn listen
  "if you listen with a listener of the same name (i.e. a service failed and was restarted, we overwrite the old listener."
  [store
   {:keys [uri name event listener type]
    :as spec}]
  (let [new-spec (->
		  spec
		  (?> type dissoc :listener)
		  (?> (not uri) assoc :uri (str "/" name)))]
    (when-not (store :bucket event)
      (store :add event))
    (store :put event name new-spec)
    (when (= :rest type)
      (future
       (-> (fn-handler
	     (:uri new-spec)
	     listener)
	   vector
	   (start-web new-spec))))
    store))

;;TODO: hacked.  make consisitant with graph observation.
(defn graph-listen [queue-spec
		    listener-spec
		    {:keys [offer in priority] :as root} & [obs]]
  (let [offer {:id :listener
	       :f (if (not priority)
		    offer
		    #(offer (priority-item priority %)))}
	{:keys [f]} (if (not obs)
		      offer
		      (obs offer))]
    (-> (store [] queue-spec)
	(listen (assoc listener-spec
		  :listener f))))
  root)

(defn listener [{:keys [listener]
		 :as spec}]
  (if listener
    listener
    (apply client-wrapper
	   (apply concat (assoc spec
			   :with-body? true)))))

(defn queue-store [s spec]
  (run-jetty
   (apply routes (rest-store-handler s))	  
   (assoc spec :join? false))
     s)

(defn notifier [store bucket & {:keys [refresh drain]}]
  (let [listeners #(map (fn [[_ spec]]
			  (->> spec
			       keywordize-map
			      listener
			      (?>> drain draining-fn drain)))
			(store :seq bucket))
	listeners
	(if refresh
	  (let [ls (refreshing-resource
		    listeners
		    refresh)]
	    (fn [] @ls))
	  listeners)]
    (fn [x]
      (doseq [listener (listeners)]
	(listener x)))))