(ns work.queue-test
  (:use clojure.test [plumbing.core :only [retry]]
	work.queue
	[ring.adapter.jetty :only [run-jetty]]
	[compojure.core :only [routes]]
	[work.graph :only [priority-in]] services.core clojure.test)
  (:require [work.queue :as work]))

(defn- basic-queue-test [q]
  (work/offer q "b")
  (work/offer-unique q "a")
  (is (= 2 (count q)))
  (is (= (work/peek q) "b"))
  (is (= (work/poll q) "b")))

(deftest local-queue-test
  (basic-queue-test (work/local-queue)))

(deftest priority-queue-test
  (let [q (work/priority-queue)]
    (work/offer q "b")
    (work/offer-unique q "a")
    (is (= 2
           (count q)))
    (is (= "a"
           (work/peek q)))
    (is (= "a"
           (work/poll q)))
    (is (= "b"
           (work/poll q)))))

(deftest priority-queue-comparator-test
  (let [q (work/priority-queue 11 >)]
    (work/offer-all q [10 300 77 10])
    (is (= 300
           (work/poll q)))
    (is (= 77
           (work/poll q)))
    (is (= 10
           (work/poll q)))
    (is (= 10
           (work/poll q)))
    (is (nil? (work/poll q))))

  (let [q (work/priority-queue 11
                               work/priority)]
    (work/offer-all q [{:priority 2
                        :val "medium"}
                       {:priority 1
                        :val "small"}
                       {:priority 3
                        :val "large"}])

    (is (= "large"
           (:val (work/poll q))))
    (is (= "medium"
           (:val (work/poll q))))
    (is (= "small"
           (:val (work/poll q))))))

(deftest put-job-test
  (let [q (work/priority-queue 10
                               work/priority)]
    (work/offer-unique q {:priority 10
			  :item "middle"})
    (work/offer-unique q {:priority 15
			  :item "front"})
    (work/offer-unique q {:priority 1
			  :item "back"})

    (is (= {:priority 15
            :item "front"}
           (work/poll q)))
    (is (= {:priority 10
            :item "middle"}
           (work/poll q)))
    (is (= {:priority 1
            :item "back"}
           (work/poll q)))))

(deftest process-job-test
  (let [q (doto (work/priority-queue
                 10 work/priority)
            (work/offer-all [{:priority 10
                              :item {:user "alice"}}
                             {:priority 10
                              :item {:user "bob"}}
                             {:priority 10
                              :item {:user "carol"}}]))
        get-job #(:item (work/poll q))
        f-result (atom [])
        cb-result (atom [])
        f #(swap! f-result conj (str "f-" (:user %)))
        cb #(swap! cb-result conj (str "cb-" (:user %)))]
    ((juxt f cb) (get-job))
    ((juxt f cb) (get-job))
    ((juxt f cb) (get-job))
    (is (= #{"f-alice" "f-bob" "f-carol"}
           (set @f-result)))
    (is (= #{"cb-alice" "cb-bob" "cb-carol"}
           (set @cb-result)))))

(deftest priority-items
  (is (= {:item 1 :priority 10}
	 (work/priority-item 10 1)))
  (is (= {:item 1 :priority 10}
	 (work/priority-item 11 {:item 1 :priority 10})))
  (is (= {:item 1 :priority 10}
	 (work/priority-item 10 {:item 1})))
  (is (= {:item {:foo 1} :priority 10}
	 (work/priority-item 10 {:foo 1}))))