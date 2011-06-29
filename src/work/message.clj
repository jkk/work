(ns work.message
  (:use [plumbing.core :only [apply-each ?>> keywordize-map]]
	[plumbing.accumulators :only [draining-fn]]
	[plumbing.cache :only [refreshing-resource]]
        [store.api :only [store]]
	[store.core :only [bucket-seq bucket-keys]]
	[services.core :only [fn-handler start-web client-wrapper]]
	[plumbing.error :only [assert-keys]]))

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
			     
(defn publisher
  [{:keys [store topic refresh drain]}]
  (assert store)
  (assert topic)
  (let [subscribers #(map (fn [[_ spec]]
			  (->> spec
			       keywordize-map
			       subscriber
			       (?>> drain draining-fn drain)))
			(store :seq topic))
	subscribers
	(if refresh	  
	  (refreshing-resource subscribers refresh)
	  subscribers)]
    (partial map-fns (subscribers))))