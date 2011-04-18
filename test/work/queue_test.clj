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

(deftest get-job-test
  (let [q (doto (work/priority-queue 10
                                     (work/by-key > :priority))
            (work/offer-all [{:priority 10
                              :job {:user "alice"}}
                             {:priority 9
                              :job {:user "bob"}}
                             {:priority 8
                              :job {:user "carol"}}]))
        refill (fn [] [[3 {:user "dick"}]])]
    (is (= {:user "alice"}
           (work/get-job refill q)))
    (is (= {:user "bob"}
           (work/get-job refill q)))
    (is (= {:user "carol"}
           (work/get-job refill q)))
    (is (= {:user "dick"}
           (work/get-job refill q)))
    (is (= {:user "dick"}
           (work/get-job refill q)))))

(deftest put-job-test
  (let [q (work/priority-queue 10
                               (work/by-key > :priority))]
    (work/put-job q 10 "middle")
    (work/put-job q 15 "front")
    (work/put-job q 1 "back")

    (is (= {:priority 15
            :job "front"}
           (work/poll q)))
    (is (= {:priority 10
            :job "middle"}
           (work/poll q)))
    (is (= {:priority 1
            :job "back"}
           (work/poll q)))))

(deftest process-job-test
  (let [q (doto (work/priority-queue
                 10 (work/by-key > :priority))
            (work/offer-all [{:priority 10
                              :job {:user "alice"}}
                             {:priority 10
                              :job {:user "bob"}}
                             {:priority 10
                              :job {:user "carol"}}]))
        get-job #(:job (work/poll q))
        f-result (atom [])
        cb-result (atom [])
        f #(swap! f-result conj (str "f-" (:user %)))
        cb #(swap! cb-result conj (str "cb-" (:user %)))]
    (work/process-job get-job f cb)
    (work/process-job get-job f cb)
    (work/process-job get-job f cb)
    (work/process-job get-job f cb)
    (is (= #{"f-alice" "f-bob" "f-carol"}
           (set @f-result)))
    (is (= #{"cb-alice" "cb-bob" "cb-carol"}
           (set @cb-result)))))

(deftest priority-process-test
  (let [done (atom [])
        data (atom ["a" "c" "d" "e"])
        [start-proc put-job] (work/priority-process
                              identity 
                              (fn []
                                (if-let [ds @data]
                                  (do (reset! data nil)
                                      (for [x ds] [10 x]))
                                  nil))
                              (partial swap! done conj))]
    
    (put-job 1 "bottom")
    (put-job 20 "top")
    
    (future (start-proc))
    (Thread/sleep 500)

    (is (= "top" (first @done)))
    (is (= "bottom" (second @done)))
    (is (= #{"a" "c" "d" "e" "bottom"}
           (set (rest @done))))))