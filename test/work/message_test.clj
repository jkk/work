(ns work.message-test
  (:use work.message store.api clojure.test services.core store.net store.core)
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
	     {:store s
	      :refresh 10
	      :topic "topic1"})]
    (Thread/sleep 500)
    (f "42")
    (is (= ["42" "42"] @a))))

(deftest rest-queue-handler-test
  (let [s (store [] {:type :mem})
	server (start-broker-server s 4446)
	other-server (atom nil)]
    (try (let [b (broker broker-spec)
	       g (->> (graph/priority-in 10 {:f identity})
		      (merge {:priority 5}))
	       _ (graph/subscribe b {:id "bar" :topic "foo"} g)
	       s1     (start-subscribers b)
	       _ (reset! other-server s1)
	       publish (publisher {:store (:remote b)
				   :refresh 10
				   :topic "foo"})]
	   (Thread/sleep 500)
	   (is (= ["foo"] (bucket-keys (.bucket-map s))))
	   (publish "id")
	   (publish "deznutz")
	   (is (= (:item ((:in g))) "id"))
	   (is (= (:item ((:in g))) "deznutz")))
	 (finally
	  (.stop server)
	  (when @other-server (.stop @other-server))))))
