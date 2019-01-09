(ns fixie.core-test
  (:require
   [fixie.core :refer :all]
   [clojure.test :refer :all]
   [testit.core :refer :all]
   [orchestra.spec.test :as st]
   [expound.alpha :as expound]))

(set! *warn-on-reflection* false)
(set! clojure.spec.alpha/*explain-out* expound/printer)
(st/instrument)

(deftest make-db-tests
  (doseq [db-type [:memory :direct-memory :heap :temp-file]
          :let [db (make-db {:db-type db-type})]]
    (is (instance? org.mapdb.DB db))
    (close! db)))

(deftest hash-map-basic-tests
  (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? false}
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
     @(empty! hm) => {}))
  (with-open [hm (open-collection! :hash-map "hash-map-tests")]
    (facts
     (get-collection-type hm) => :hash-map
     (get-collection-name hm) => "hash-map-tests"
     (get-collection-options hm) =in=> {:counter-enable? true})))

(deftest hash-map-coll-ops
  (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? false}
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

     (seq hm) =in=> ^:in-any-order [[1 1] [2 2] [3 3]])))

(deftest hash-map-custom-serializers
  (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? false}
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
     (map identity hm) =in=> ^:in-any-order [[{:mykey 42} {:myval 42}]])))

(deftest hash-map-concurrency
  (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? true}
                                   :hash-map "hash-map-tests"
                                   {:counter-enable? true})]
    (assoc! hm :slow 0)
    (future
      (facts
       (update! hm :slow (fn [old] (Thread/sleep 200) (+ old 10))) =throws=> java.util.ConcurrentModificationException))
    (Thread/sleep 100)
    (assoc! hm :slow 42)
    (Thread/sleep 300)
    (is (= (:slow hm) 42))))

(deftest hash-map-transaction
  (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? true}
                                   :hash-map "hash-map-tests"
                                   {:counter-enable? true})]
    (facts
     @(assoc! hm :0 0) => {:0 0}
     (commit! hm) => any
     @(assoc! hm :1 1) =in=> {:1 1}
     (rollback! hm) => any
     @hm => {:0 0})))

(deftest tree-map-basic-tests
  (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? false}
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
     @(empty! hm) => {})))

(deftest tree-map-coll-ops
  (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? false}
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
    (with-open [hm2 (open-collection! {:db-type :temp-file :transaction-enable? false}
                                      :tree-map "tree-map-tests"
                                      {:counter-enable? true
                                       :key-serializer :long
                                       :value-serializer :long
                                       :initial-content hm})]
      (facts
       (seq hm2) => (seq hm)))))

(deftest tree-map-custom-serializers
  (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? false}
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
     (map identity hm) =in=> ^:in-any-order [[{:mykey 42} {:myval 42}]])))

(deftest tree-map-concurrency
  (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? true}
                                   :tree-map "tree-map-tests"
                                   {:counter-enable? true})]
    (assoc! hm :slow 0)
    (future
      (facts
       (update! hm :slow (fn [old] (Thread/sleep 200) (+ old 10))) =throws=> java.util.ConcurrentModificationException))
    (Thread/sleep 100)
    (assoc! hm :slow 42)
    (Thread/sleep 300)
    (is (= (:slow hm) 42))))

(deftest missing-serializer
  (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? true}
                                   :tree-map "tree-map-tests"
                                   {:counter-enable? true
                                    :key-serializer :nippy
                                    :value-serializer :nippy})]
    (facts
     (assoc! hm :foo [45 98]) =throws=> clojure.lang.ExceptionInfo)))

(deftest custom-serializer
    (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? true}
                                     :tree-map "tree-map-tests"
                                     {:key-serializer :edn
                                      :value-serializer {:raw-serializer :string
                                                         :wrapper-serializer
                                                         {:encoder (fn mapdb-edn-encoder [v]
                                                                     (pr-str v))
                                                          :decoder (fn mapdb-edn-decoder [v]
                                                                     (read-string v))}}})]
    (facts
     @(assoc! hm :foo [45 98]) => {:foo [45 98]})))


(deftest test-into
  (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? true}
                                   :hash-map "hash-map-tests")
              tm (open-collection! {:db-type :temp-file :transaction-enable? true}
                                   :tree-map "tree-map-tests")]
    (let [add-map {:foo 42 :bar {'baz "str"}}
          add-keys [[:foo 42] [:bar {'baz "str"}]]]
      (facts
       @(into! hm add-map) => add-map
       @(empty! hm) => {}
       @(into! hm add-keys) => add-map
       @(into! tm add-map) => add-map
       @(empty! tm) => {}
       @(into! tm add-keys) => add-map))))

(deftest modification-listener
  (let [res (atom nil)]
    (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? true}
                                     :hash-map "hash-map-tests"
                                     {:modification-listener
                                      (fn [k old-v new-v triggered?]
                                        (reset! res [k old-v new-v triggered?]))})]
      (facts
       @(assoc! hm :foo 42) => {:foo 42}
       @res => [:foo nil 42 false]
       (reset! res nil) => nil
       @(empty! hm) => {}
       @res => nil
       @(assoc! hm :bar 42) => {:bar 42}
       @(update! hm :bar inc) => {:bar 43}
       @res => [:bar 42 43 false]
       @(empty! hm true) => {}
       @res => [:bar 43 nil true]))))

(deftest value-loader
  (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? true}
                                   :hash-map "hash-map-tests"
                                   {:value-loader
                                    (fn [k] (count (name k)))})]
    (facts
     (get hm :foo) => 3
     (hm :bar) => 3
     @(update! hm :baz inc) =in=> {:baz 4}
     @hm => {:foo 3 :bar 3 :baz 4})))

(deftest expiration-size-test
  (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? true}
                                   :hash-map "hash-map-tests"
                                   {:expire-max-size 1
                                    :expire-executor 2
                                    :expire-executor-period 10
                                    :expire-after-create nil})]
    (assoc! hm :toto 42)
    (assoc! hm :bar 50)
    (assoc! hm :baz 50)
    (assoc! hm :titi 67)
    (Thread/sleep 100)
    (facts
     (count hm) => #(< % 4))))

(deftest expiration-time-test
  (with-open [hm (open-collection! {:db-type :temp-file :transaction-enable? true}
                                   :hash-map "hash-map-tests"
                                   {:expire-after-create [10 :ms]
                                    :expire-executor 2
                                    :expire-executor-period 10})]
    (assoc! hm :toto 42)
    (Thread/sleep 100)
    (facts
     (count hm) => 0)))
