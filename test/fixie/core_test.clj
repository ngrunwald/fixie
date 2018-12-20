(ns fixie.core-test
  (:require
   [fixie.core :refer :all]
   [clojure.test :refer :all]
   [testit.core :refer :all]
   [orchestra.spec.test :as st]
   [expound.alpha :as expound]))

(set! *warn-on-reflection* true)
(set! clojure.spec.alpha/*explain-out* expound/printer)
(st/instrument)

(deftest make-db-tests
  (doseq [db-type [:memory :direct-memory :heap :temp-file]
          :let [db (make-db {:db-type db-type})]]
    (is (instance? org.mapdb.DB db))
    (close db)))

(deftest hash-map-basic-tests
  (let [hm (open-collection! {:db-type :temp-file :transaction-enable? false}
                             :hash-map "hash-map-tests" {:counter-enable? true})]
    (facts
     (get-collection-name hm) => "hash-map-tests"
     (get-db hm) => #(instance? org.mapdb.DB %)
     (get-db-options hm) =in=> {:db-type :temp-file :transaction-enable? false}
     (get-collection hm) => #(instance? org.mapdb.HTreeMap %)
     (get-collection-type hm) => :hash-map
     (get-collection-options hm) =in=> {:counter-enable? true}
     (empty? hm) => true
     (count hm) => 0
     @(assoc! hm :foo 42) => {:foo 42}
     (count hm) => 1
     (:foo hm) => 42
     (hm :foo) => 42
     (get hm :foo) => 42
     (hm :not-here :default) => :default
     @(update! hm :foo inc) => {:foo 43}
     @(update! hm :foo + 2) => {:foo 45}
     @(update! hm :nobod (constantly :here)) =in=> {:nobod :here}
     @(update-in! hm [:bar :baz] assoc :fizz :buzz) =in=> {:bar {:baz {:fizz :buzz}}}
     (get-in hm [:bar :baz :fizz]) => :buzz
     (keys hm) =in=> ^:in-any-order [:bar :nobod :foo]
     (vals hm) =in=> ^:in-any-order [45 :here {:baz {:fizz :buzz}}]
     @(dissoc! hm :bar :nobod :foo) => {}
     (empty? hm) => true
     (count hm) => 0
     (close hm) => nil)))

(deftest hash-map-coll-ops
  (let [hm (open-collection! {:db-type :temp-file :transaction-enable? false}
                             :hash-map "hash-map-tests"
                             {:counter-enable? true
                              :key-serializer :long
                              :value-serializer :long})]
    (facts
     (seq hm) => nil

     @(assoc! hm 1 1 2 2 3 3) => #(not (empty? %))

     (reduce-kv
      (fn [acc k v] (assoc acc (inc k) (inc v)))
      {} hm) => {2 2 3 3 4 4}

     (reduce
      (fn [acc [k v]] (assoc acc (inc k) (inc v)))
      {} hm) => {2 2 3 3 4 4}

     (map identity hm) =in=> ^:in-any-order [[1 1] [2 2] [3 3]]

     (seq hm) =in=> ^:in-any-order [[1 1] [2 2] [3 3]]
     (close hm) => nil)))

(deftest hash-map-custom-serializers
  (let [hm (open-collection! {:db-type :temp-file :transaction-enable? false}
                             :hash-map "hash-map-tests"
                             {:counter-enable? true
                              :key-serializer :edn
                              :value-serializer :edn})]
    (facts
     (get-collection-options hm) =in=> {:key-serializer-wrapper
                                        {:encoder fn?
                                         :decoder fn?}
                                        :value-serializer-wrapper
                                        {:encoder fn?
                                         :decoder fn?}
                                        :key-serializer :string
                                        :value-serializer :string}
     @(assoc! hm {:mykey 42} {:myval 42}) => {{:mykey 42} {:myval 42}}
     (hm {:mykey 42}) => {:myval 42}
     (map identity hm) =in=> ^:in-any-order [[{:mykey 42} {:myval 42}]]
     (close hm) => nil)))

(deftest hash-map-concurrency
  (let [hm (open-collection! {:db-type :temp-file :transaction-enable? true}
                             :hash-map "hash-map-tests"
                             {:counter-enable? true})]
    (assoc! hm :slow 0)
    (future
      (facts
       (update! hm :slow (fn [old] (Thread/sleep 200) (+ old 10))) =throws=> java.util.ConcurrentModificationException))
    (Thread/sleep 100)
    (assoc! hm :slow 42)
    (Thread/sleep 300)
    (is (= (:slow hm) 42))
    (close hm)))

(deftest hash-map-transaction
  (let [hm (open-collection! {:db-type :temp-file :transaction-enable? true}
                             :hash-map "hash-map-tests"
                             {:counter-enable? true})]
    (facts
     @(assoc! hm :0 0) => {:0 0}
     (commit! hm) => any
     @(assoc! hm :1 1) =in=> {:1 1}
     (rollback! hm) => any
     @hm => {:0 0})))



(deftest tree-map-basic-tests
  (let [hm (open-collection! {:db-type :temp-file :transaction-enable? false}
                             :tree-map "tree-map-tests" {:counter-enable? true})]
    (facts
     (get-collection-name hm) => "tree-map-tests"
     (get-db hm) => #(instance? org.mapdb.DB %)
     (get-db-options hm) =in=> {:db-type :temp-file :transaction-enable? false}
     (get-collection hm) => #(instance? org.mapdb.BTreeMap %)
     (get-collection-type hm) => :tree-map
     (get-collection-options hm) =in=> {:counter-enable? true}
     (empty? hm) => true
     (count hm) => 0
     @(assoc! hm :foo 42) => {:foo 42}
     (count hm) => 1
     (:foo hm) => 42
     (hm :foo) => 42
     (get hm :foo) => 42
     (hm :not-here :default) => :default
     @(update! hm :foo inc) => {:foo 43}
     @(update! hm :foo + 2) => {:foo 45}
     @(update! hm :nobod (constantly :here)) =in=> {:nobod :here}
     @(update-in! hm [:bar :baz] assoc :fizz :buzz) =in=> {:bar {:baz {:fizz :buzz}}}
     (get-in hm [:bar :baz :fizz]) => :buzz
     (keys hm) =in=> ^:in-any-order [:bar :nobod :foo]
     (vals hm) =in=> ^:in-any-order [45 :here {:baz {:fizz :buzz}}]
     @(dissoc! hm :bar :nobod :foo) => {}
     (empty? hm) => true
     (count hm) => 0
     (close hm) => nil)))

(deftest tree-map-coll-ops
  (let [hm (open-collection! {:db-type :temp-file :transaction-enable? false}
                             :tree-map "tree-map-tests"
                             {:counter-enable? true
                              :key-serializer :long
                              :value-serializer :long})]
    (facts
     (seq hm) => nil

     @(assoc! hm 1 1 2 2 3 3) => #(not (empty? %))

     (reduce-kv
      (fn [acc k v] (assoc acc (inc k) (inc v)))
      {} hm) => {2 2 3 3 4 4}

     (reduce
      (fn [acc [k v]] (assoc acc (inc k) (inc v)))
      {} hm) => {2 2 3 3 4 4}

     (map identity hm) =in=> ^:in-any-order [[1 1] [2 2] [3 3]]

     (seq hm) =in=> ^:in-any-order [[1 1] [2 2] [3 3]])

    ;; test :initial-content
    (let [hm2 (open-collection! {:db-type :temp-file :transaction-enable? false}
                                :tree-map "tree-map-tests"
                                {:counter-enable? true
                                 :key-serializer :long
                                 :value-serializer :long
                                 :initial-content hm})]
      (facts
       (seq hm2) => (seq hm)
       (close hm2) => nil
       (close hm) => nil))))

(deftest tree-map-custom-serializers
  (let [hm (open-collection! {:db-type :temp-file :transaction-enable? false}
                             :tree-map "tree-map-tests"
                             {:counter-enable? true
                              :key-serializer :edn
                              :value-serializer :edn})]
    (facts
     (get-collection-options hm) =in=> {:key-serializer-wrapper
                                        {:encoder fn?
                                         :decoder fn?}
                                        :value-serializer-wrapper
                                        {:encoder fn?
                                         :decoder fn?}
                                        :key-serializer :string
                                        :value-serializer :string}
     @(assoc! hm {:mykey 42} {:myval 42}) => {{:mykey 42} {:myval 42}}
     (hm {:mykey 42}) => {:myval 42}
     (map identity hm) =in=> ^:in-any-order [[{:mykey 42} {:myval 42}]]
     (close hm) => nil)))

(deftest tree-map-concurrency
  (let [hm (open-collection! {:db-type :temp-file :transaction-enable? true}
                             :tree-map "tree-map-tests"
                             {:counter-enable? true})]
    (assoc! hm :slow 0)
    (future
      (facts
       (update! hm :slow (fn [old] (Thread/sleep 200) (+ old 10))) =throws=> java.util.ConcurrentModificationException))
    (Thread/sleep 100)
    (assoc! hm :slow 42)
    (Thread/sleep 300)
    (is (= (:slow hm) 42))
    (close hm) => nil))

(deftest missing-serializer
  (let [hm (open-collection! {:db-type :temp-file :transaction-enable? true}
                             :tree-map "tree-map-tests"
                             {:counter-enable? true
                              :key-serializer :nippy
                              :value-serializer :nippy})]
    (facts
     (assoc! hm :foo [45 98]) =throws=> clojure.lang.ExceptionInfo
     (close hm) => nil)))
