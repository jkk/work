(ns work.queue-test
  (:use clojure.test
        [plumbing.core :only [retry]])
  (:require [work.queue :as work]))

(defn- basic-queue-test [q]
  (work/offer q "b")
  (work/offer-unique q "a")
  (is (= 2 (count q)))
  (is (= (work/peek q) "b"))
  (is (= (work/poll q) "b")))

(deftest local-queue-test
  (basic-queue-test (work/local-queue)))

(deftest local-queue-with-serializer-test
  (basic-queue-test (work/with-serialize (work/local-queue))))

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
                               (work/by-key > :priority))]
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