(ns work.message
  (:use [plumbing.core :only [apply-each ?>> keywordize-map wait-until]]
	[plumbing.error :only [with-ex with-give-up]]
	[plumbing.accumulators :only [draining-fn]]
	[plumbing.cache :only [refreshing-resource]]
        [store.api :only [store mirror-remote]]
	[store.core :only [bucket-seq bucket-keys]]
	[services.core :only [fn-handler start-web client-wrapper]]
	[plumbing.error :only [assert-keys]]
	[work.core :only [schedule-work]])
  (:require [clojure.contrib.logging :as log]
	    [clj-time.core :as time]
	    [clj-time.coerce :as time-coerce]))

(defn sub-broker
  [{:keys [remote subscriber]}]
  (assert-keys [:host, :port] remote)
  (assert-keys [:host, :port, :id] subscriber)
  {:remote (store [] remote)
   :local  (store [])
   :subscriber subscriber})

(defn pub-broker
  [{:keys [remote]}]
  (assert-keys [:host, :port] remote)
  {:remote (mirror-remote remote)
   :local  (store [])})

(defn map-fns [fs x]
  (doseq [f fs] (f x)))

(defn add-subscriber
  "subscribe to a topic. subsriber is spec
    how to reach the subscriber"
  [store {:keys [id topic] :as subscriber}]
  (assert-keys [:id :topic] subscriber)
  (when-not (store :bucket topic)
    (store :add topic))
  (store :put topic id
	 (assoc subscriber
	   :started (time-coerce/to-string (time/now)))))
    
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

(defn add-publisher [{:keys [remote local] :as broker}
		     {:keys [topic drain]}]
  (when-not (local :bucket topic)
    (local :add topic))
  (doseq [[id spec] (remote :seq topic)]
    (let [cur (local :get topic id)]
      (when (or (nil? cur) (not= (:spec cur) spec))
	(local :put topic id {:spec spec
			      :f (subscriber-sender spec drain 5
						    (constantly nil))})))))

;;TODO: shoudl close over the multimap, not rebuild it on every call.
(defn publisher
  [{:keys [local] :as broker}
   {:keys [topic] :as config}]
  (assert local)
  (assert topic)
  (add-publisher broker config)
  (fn [msg]
    (doseq [[id {:keys [f]}] (local :seq topic)]
      (when f (f msg)))))


;; (defn nil-publisher [local topic id spec]
;;   (let [local-spec (local :get topic id)]
;;     (when (= local-spec spec)
;;       (local :put topic id
;; 	     (assoc local-spec :f nil)))))

;; (defn scheduled-sync [{:keys [remote local] :as broker}]
;;   (work.core/schedule-work
;;    (with-ex #(doseq [[id spec] (remote :seq topic)
;; 		     :let [cur (local :get topic id)
;; 			   pub (nil-publisher local topic id spec)]
;; 		     :when (or (nil? cur) (not= (:spec cur) spec))]
;; 	       (local :put topic id {:spec spec
;; 				     :f (subscriber-sender spec drain 5 pub)})))
;;    (or refresh 10)))