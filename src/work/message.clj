(ns work.message
  (:use [plumbing.core :only [apply-each ?>> keywordize-map wait-until]]
	[plumbing.error :only [with-ex with-give-up]]
	[plumbing.accumulators :only [draining-fn]]
	[plumbing.cache :only [refreshing-resource]]
        [store.api :only [store]]
	[store.core :only [bucket-seq bucket-keys]]
	[services.core :only [fn-handler start-web client-wrapper]]
	[plumbing.error :only [assert-keys]]
	[work.core :only [schedule-work]])
  (:require [clojure.contrib.logging :as log]))

(defn message-merge [_ old new]
  (conj (or old []) new))

;; subscriber-machine side

(defn broker
  "remote: ip specification
   local: ip specification + id"
  [{:keys [remote subscriber]}]
  (assert-keys [:host, :port] remote)
  (assert-keys [:host, :port, :id] subscriber)
  {:remote (store [] remote)
   :local  (store [] {:merge message-merge :type :mem})
   :subscriber subscriber})

(defn map-fns [fs x]
  (doseq [f fs] (f x)))

(defn add-subscriber
  "subscribe to a topic. subsriber is spec
    how to reach the subscriber"
  [store {:keys [id topic] :as subscriber}]
  (assert-keys [:id :topic] subscriber)
  (when-not (store :bucket topic)
    (store :add topic))
  (store :put topic id subscriber))
    
(defn topic-notifiers
  "returns map from topic -> notify subscribers"
  [store]
  (->> (.bucket-map store)   
       bucket-seq
       (map (fn [[topic {:keys [read]}]]
	      (assert read)
	      (assert topic)
	      (let [subscriber-fns (map (comp :f second) (bucket-seq read))]
		[topic (partial map-fns subscriber-fns)])))
       (into {})))

(defn start-subscribers
  "remote and local are stores"
  [{:keys [subscriber local remote]}]
  (assert-keys [:host :port] subscriber)
  (let [handlers (->> (topic-notifiers local)
		      (map (fn [[topic notify]]
			     (fn-handler (str "/" topic) notify))))
	 server (start-web handlers (assoc subscriber :join? false))]
    (doseq [topic (bucket-keys (.bucket-map local))]
      (add-subscriber remote
		      (assoc subscriber
			:topic topic
			:uri (str "/" topic))))
    server))

;; Publisher Side

(defn subscriber 
  [{:keys [subscriber] :as spec}]
  (assert-keys [:host :port :uri] spec)
  (or subscriber
      (apply client-wrapper
	     (apply concat (assoc spec :with-body? true)))))

(defn subscriber-sender [spec drain max-tries on-fail]
  (->> spec
       keywordize-map
       subscriber
       (?>> drain draining-fn drain)
       (with-give-up max-tries on-fail)))

(defn publisher
  [{:keys [store topic refresh drain max-tries]}]
  (assert store)
  (assert topic)
  (let [m (java.util.concurrent.ConcurrentHashMap.)
	on-fail (fn [id spec]
		  (when (= (store :get topic id) spec)
		    (.put m id (assoc (.get m id) :f nil))
		    (log/info (format "removing subscriber %s from topic %s with spec %s"
				      id topic (pr-str spec)))))
	set? (atom false)]
    (work.core/schedule-work
     #(try
	(doseq [[id spec] (store :seq topic)
		:let [cur (.get m id)]
		:when (or (nil? cur) (not= (:spec cur) spec))]
	  (.put m id {:spec spec
		      :f (subscriber-sender spec drain 5 (fn [] (on-fail id spec)))}))
	(catch Exception e (.printStackTrace e))
	(finally (reset! set? true)))
     (or refresh 10))    
    (fn [msg]
      (wait-until #(deref set?))
      (let [fs (for [{:keys [f]} (.values m) :when f] f)]
	(doseq [f fs] (f msg))))))
	  