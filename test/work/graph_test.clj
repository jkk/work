(ns work.graph-test
  (:require [work.queue :as q]
	    [clojure.zip :as zip])
  (:use clojure.test
	plumbing.core
	work.graph))

;;TODO: copied from core-test, need to factor out to somehwere.
(defn wait-for-complete-results
  "Test helper fn waits until the pool finishes processing before returning results."
  [response-q expected-seq-size]
  (wait-until #(= (.size response-q) expected-seq-size) 5)
  (sort (iterator-seq (.iterator response-q))))

(defn queue-seq [q]
  (sort (iterator-seq (.iterator q))))

(deftest graph-comp-test
  (let [inc-graph
	{:f inc
	 :children [{:f inc} {:f identity}]}]
    (is (= [5 4] ((graph-comp inc-graph) 3)))))

(deftest one-node-graph-test
  (let [incq (q/local-queue)
	idq (q/local-queue)
	root (-> (root-node)
		 graph-zip
		 (child (node (out inc incq)
			      :id :inc))
		 (child (node (out identity idq)
			      :id :identity )))]
    (mono-run root (range 5))
    (is (= (range 1 6) (wait-for-complete-results incq 5)))
    (is (= (range 5) (wait-for-complete-results idq 5)))))

#_(deftest one-node-graph-test
  (let [root (-> (root-node)
		 (drain-to-vertex (range 5))
		 graph-zip
		 (child (node :f inc :id :inc))
		 (child (node :id :identity))
		 run-graph)
	out (terminal-queues root)]
    (is (= (range 1 6) (wait-for-complete-results (:inc out) 5)))
    (is (= (range 5) (wait-for-complete-results (:identity out) 5)))
    (kill-graph root)))

;; (deftest multimap-graph-test
;;   (let [root (-> (root-node)
;; 		 (drain-to-vertex (range 4))
;; 		 graph-zip
;; 		 (add-edge (node :f identity
;; 					  :id :multimap
;; 					  :multimap (fn [x] (range x))))
;; 		 run-graph)
;; 	out (terminal-queues root)]
;;     (is (= [0 0 0 1 1 2] (wait-for-complete-results (:multimap out) 5)))
;;     (kill-graph root)))

;; (deftest chain-graph-test
;;   ; (range 5) -> inc -> inc
;;   (let [root (-> (root-node)
;; 		 (drain-to-vertex (range 5))
;; 		 graph-zip
;; 		 (add-edge-> (node inc))
;; 		 (add-edge (node :f inc :id :out))
;; 		 (add-edge (node :f (partial + 2) :id :plus-two))
;; 		 run-graph)
;; 	outs (terminal-queues root)]
;;     (is (= (range 2 7) (wait-for-complete-results (:out outs) 5)))
;;     (is (= (range 3 8) (wait-for-complete-results (:plus-two outs) 5)))
;;     (kill-graph root)))

;; (deftest graph-test
;;   (let [input-data (range 1 101 1)
;;         root (-> (root-node)
;; 		 (drain-to-vertex input-data)
;; 		 graph-zip
;; 		 (add-edge-> (node (partial * 10) :threads 2))
;; 		 (add-edge (node :f inc :id :output))
;; 		 run-graph)
;; 	out (terminal-queues root)]
;;     (is (= (map (fn [x] (inc (* 10 x))) (range 1 101 1))
;; 	   (wait-for-complete-results (:output out) (count input-data))))
;;     (kill-graph root)))

;; (deftest simple-dispatch-test
;;   (let [root (-> (root-node)
;; 		 (drain-to-vertex [1 2 3 4 5])
;; 		 graph-zip
;; 		 (add-edge-> (node identity))
;; 		 (add-edge (node :id :even)
;; 			   :when even?)
;; 		 (add-edge (node :id :odd)
;; 			   :when odd?)
;; 		 (add-edge (node :id :all))
;; 		 (add-edge (node :f inc :id :plus-1)
;; 			   :when odd?)
;; 		 run-graph)
;; 	outs (terminal-queues root)]
;;     (Thread/sleep 200)
;;     (kill-graph root)
;;     (is (= (-> outs :even seq sort) [2 4]))
;;     (is (= (-> outs :odd seq sort) [1 3 5]))
;;     (is (= (-> outs :all seq sort) (range 1 6)))
;;     (is (= (-> outs :plus-1 seq sort) (map inc [1 3 5])))))

;; (deftest simple-sideffect-test
;;   (let [a (atom nil)
;; 	root (-> (root-node)
;; 		 (drain-to-vertex [1 2 3 4 5])
;; 		 graph-zip
;; 		 (add-edge-> (fn [x] (swap! a conj x)))
;; 		 run-graph)]
;;     (Thread/sleep 2000)
;;     (is (= (sort @a) [1 2 3 4 5]))))