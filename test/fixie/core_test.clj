(ns fixie.core-test
  (:require
   [fixie.core :refer :all]
   [clojure.test :refer :all])
  (:import [fixie.core MapDB DBHashMap TransactionMapDB]))


(with-test
  (def mdb (mapdb :memory {:cache-disable true}))
  (is (instance? MapDB mdb))
  (is (not (closed? mdb))))

(with-test
  (def hmap1 (:test1 (assoc! mdb :test1 {:type :hash-map :counter-enable true})))
  (commit! mdb)
  (is (instance? DBHashMap hmap1))
  (is (empty? hmap1))
  (is (= 0 (count hmap1)))
  (is (= "test1" (name hmap1)))

  (is (= {:toto 42} (into {} (assoc! hmap1 :toto 42))))
  (is (= 42 (get hmap1 :toto)))
  (is (= 42 (hmap1 :toto)))
  (is (= ::test (get hmap1 ::no ::test)))
  (is (= ::test (hmap1 ::no ::test)))
  (is (= 42 (:toto hmap1)))
  (is (= 1 (count hmap1)))
  (is (= '(:toto) (keys hmap1)))
  (is (= '(42) (vals hmap1)))
  (rollback! mdb)
  (is (= 0 (count (keys hmap1))))
  (assoc! hmap1 :toto 42 :titi 11)
  (commit! hmap1)
  (is (= 53 (reduce (fn [acc [k v]] (+ acc v)) 0 hmap1)))
  (rollback! hmap1)
  (is (= 2 (count hmap1)))
  (is (= 22 (:titi (update-in! hmap1 [:titi] * 2))))
  (let [hm (update-in! hmap1 [:l1 :l2 :l3] (constantly "toto"))]
    (is (= "toto" (get-in hm [:l1 :l2 :l3])))))


;; (with-test
;;   (def tdb (mapdb :memory {:fully-transactional? true}))
;;   (is (instance? TransactionMapDB tdb))
;;   (with-tx [tx tdb]
;;     (let [hm1 (get tx :test1 {:type :tree-map})]
;;       (assoc! hm1 :foo "bar")))
;;   (with-tx [tx1 tdb] 42)
;;   ;; (is "bar" (with-tx [tx tdb] (println "toto")))
;;   )
