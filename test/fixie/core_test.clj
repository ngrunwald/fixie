(ns fixie.core-test
  (:require
   [fixie.core :refer :all]
   [clojure.test :refer :all])
  (:import [fixie.core MapDB DBHashMap]))


(with-test
  (def mdb (mapdb :memory {:cache-disable true}))
  (is (instance? MapDB mdb))
  (is (not (closed? mdb))))

(with-test
  (def hmap1 (get mdb :test1 {:type :hash-map :counter-enable true}))
  (is (instance? DBHashMap hmap1))
  (is (empty? hmap1))
  (is (= 0 (count hmap1)))
  (is (= "test1" (name hmap1)))
  ;; (is (= (get mdb "test1") hmap1))

  (is (= {:toto 42} (into {} (assoc! hmap1 :toto 42))))
  (is (= 42 (get hmap1 :toto)))
  (is (= 42 (hmap1 :toto)))
  (is (= ::test (get hmap1 ::no ::test)))
  (is (= ::test (hmap1 ::no ::test)))
  (is (= 42 (:toto hmap1)))
  (is (= 1 (count hmap1)))
  (rollback! mdb)
  (println (keys hmap1))
  )

;; (let [coll2 (assoc! (get mdb :test2 {:type :tree-map :counter-enable true}) :toto 42)]
;;   (expect 42 (coll2 :toto))
;;   (expect 42 (get coll2 :toto))
;;   (expect ::test (get coll2 :toti ::test))
;;   (expect 1 (count coll2))
;;   (expect #{:toto} (into #{} (keys coll2)))
;;   (expect #{42} (into #{} (vals coll2))))

;; (let [coll3 (get mdb :test3 {:type :tree-map :counter-enable true})]
;;   (update-in! coll3  [:toto] #(conj )))
