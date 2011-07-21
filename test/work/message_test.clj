(ns work.message-test
  (:use work.message
	store.api
	clojure.test
	[services.core :only [start-web]]
	store.net
	store.core)
  (:require [work.graph :as graph]))

(def broker-spec
     {:remote {:host "localhost"
	       :port 4446
	       :type :rest}
      :subscriber {:host "localhost"
		   :port 4456
		   :type :rest
		   :id "service-id"}})

(defn start-broker-server [s port]
  (start-web (rest-store-handler s) {:port port :join? false}))

(deftest add-subscriber-test
  (let [s (store [] {:type :mem})]
    (add-subscriber s {:id "me" :topic "balls" :host "localhost"})
    (is (= ["me"] (s :keys "balls")))
    (is (= {:id "me" :topic "balls" :host "localhost"}
	   (select-keys (s :get "balls" "me")
			[:id, :topic, :host])))
    (add-subscriber s {:id "me" :topic "balls" :host "localhost"})
    (is (= {:id "me" :topic "balls" :host "localhost"}
	   (select-keys (s :get "balls" "me") [:id, :topic, :host])))))

(deftest topic-notifiers-test
  (let [a1 (atom [])
	f (partial swap! a1 conj)
	s (store [] {:type :mem})
	_ (add-subscriber s {:id "me" :topic "topic" :f f})
	n (topic-notifiers s)]
    ((n "topic") 42)
    (is (= [42] @a1))))

(deftest publisher-test
  (let [s (store [] {:type :mem})
	topic "topic1"
	a (atom [])
	f (partial swap! a conj)
	_ (add-subscriber s {:id "1"
			     :host "localhost"
			     :port 42
			     :uri "/topic1"
			     :topic "topic1"
			     :subscriber f })
	_ (add-subscriber s {:id "2"
			     :host "localhost"
			     :port 42
			     :topic "topic1"
			     :uri "/topic1"
			     :subscriber f })
	f (publisher
	   {:local s
	    :remote s}
	   {:topic "topic1"})]
    (Thread/sleep 500)
    (f "42")
    (is (= ["42" "42"] @a))))

(deftest rest-queue-handler-test
  (let [s (store [] {:type :mem})
	server (start-broker-server s 4446)
	other-server (atom nil)]
    (try (let [sub-b (sub-broker broker-spec)
	       g (-> {:f identity}
		     (graph/priority-in 10)
		     (graph/subscribe sub-b {:id "bar" :topic "foo"}))
	       s1     (start-subscribers sub-b)
	       _ (reset! other-server s1)]
	   (Thread/sleep 500)
	   (is (= ["foo"] (bucket-keys (.bucket-map s))))
	   (let [pub-b (pub-broker broker-spec)
		 publish (publisher pub-b
				    {:topic "foo"})]
	     (publish "id")
	     (publish "deznutz")
	     (is (= (:item ((:in g))) "id"))
	     (is (= (:item ((:in g))) "deznutz"))))
	 (finally
	  (.stop server)
	  (when @other-server (.stop @other-server))))))

(deftest stress-rest-queue-handler-test
  (let [s (store [] {:type :mem})
	server (start-broker-server s 4446)
	other-server (atom nil)]
    (try (let [sub-b (sub-broker broker-spec)
	       g (-> {:f identity}
		     (graph/priority-in 10)
		     (graph/subscribe sub-b {:id "bar" :topic "foo"}))
	       s1     (start-subscribers sub-b)
	       _ (reset! other-server s1)]
	   (Thread/sleep 500)
	   (let [pub-b (pub-broker broker-spec)
		 publish (publisher pub-b
				    {:topic "foo"})]
	     (dotimes [i 5000]
	       (Thread/sleep 10)
	       (publish (str i)))
	     (Thread/sleep 2000)
	     (is (= (count (:queue g)) 5000))
	     (println (take 5 (:queue g)))
	    (is (= (map str (range 5000)) (sort-by #(Integer/parseInt %) (map :item (:queue g)))))))
	 (finally
	  (.stop server)
	  (when @other-server (.stop @other-server))))))
