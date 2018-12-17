(ns fixie.core
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s])
  (:import [org.mapdb DBMaker DBMaker$Maker DB DB$HashMapMaker
            HTreeMap DB$TreeMapMaker BTreeMap Serializer MapExtra]))

(defn- configure-maker!
  [opts-map dbm opts]
  (doseq [setter-name (keys opts)
          :let [setter (opts-map setter-name)]
          :when setter
          :let [value (opts setter-name)]]
   (setter dbm value)))

(defmacro ^:private boolean-setter
  [meth tag]
  (let [tagged-db-name (vary-meta (gensym "db") assoc :tag tag)]
    `(fn [~tagged-db-name v#]
       (when v#
         (. ~tagged-db-name ~meth))
       ~tagged-db-name)))

(defmacro ^:private sdef-enum
  [nam enum]  
  (let []
    `(s/def ~nam ~(eval enum))))

(def ^:private mapdb-types {:file (fn [{:keys [file]}]
                          (let [f (io/file file)]
                            (io/make-parents f)
                            (DBMaker/fileDB f)))
                  :heap (fn [_] (DBMaker/heapDB))
                  :memory (fn [_] (DBMaker/memoryDB))
                  :direct-memory (fn [_] (DBMaker/memoryDirectDB))
                  :temp-file (fn [_] (DBMaker/tempFileDB))})

(def ^:private mapdb-options
  {:file-mmap-enable-if-supported? (boolean-setter fileMmapEnableIfSupported
                                                   org.mapdb.DBMaker$Maker)
   :read-only? (boolean-setter readOnly
                               org.mapdb.DBMaker$Maker)
   :checksum-header-bypass? (boolean-setter checksumHeaderBypass
                                            org.mapdb.DBMaker$Maker)
   :checksum-store-enable? (boolean-setter checksumStoreEnable
                                           org.mapdb.DBMaker$Maker)
   :cleaner-hack-enable? (boolean-setter cleanerHackEnable
                                         org.mapdb.DBMaker$Maker)
   :close-on-jvm-shutdown? (boolean-setter closeOnJvmShutdown
                                           org.mapdb.DBMaker$Maker)
   :close-on-jvm-shutdown-weak-ref? (boolean-setter closeOnJvmShutdownWeakReference
                                                    org.mapdb.DBMaker$Maker)
   :concurrency-scale (fn [^DBMaker$Maker db segment-count]
                        (.concurrencyScale db segment-count))
   :executor-enable? (boolean-setter executorEnable
                                     org.mapdb.DBMaker$Maker)
   :transaction-enable? (boolean-setter transactionEnable
                                        org.mapdb.DBMaker$Maker)})

(s/def :mapdb/file-mmap-enable-if-supported? boolean?)
(s/def :mapdb/read-only? boolean?)
(s/def :mapdb/checksum-header-bypass? boolean?)
(s/def :mapdb/checksum-store-enable? boolean?)
(s/def :mapdb/cleaner-hack-enable? boolean?)
(s/def :mapdb/close-on-jvm-shutdown? boolean?)
(s/def :mapdb/close-on-jvm-shutdown-weak-ref? boolean?)
(s/def :mapdb/concurrency-scale int?)
(s/def :mapdb/executor-enable? boolean?)
(s/def :mapdb/transaction-enable? boolean?)
(s/def :mapdb/file (s/or :path string?
                         :file #(instance? java.io.File %)))
(sdef-enum :mapdb/db-type (into #{} (keys mapdb-types)))

(defmulti db-type :db-type)

(defmethod db-type :file [_]
  (s/keys :req-un [:mapdb/db-type
                   :mapdb/file]
          :opt-un [:mapdb/file-mmap-enable-if-supported?
                   :mapdb/read-only?
                   :mapdb/checksum-header-bypass?
                   :mapdb/checksum-store-enable?
                   :mapdb/cleaner-hack-enable?
                   :mapdb/close-on-jvm-shutdown?
                   :mapdb/close-on-jvm-shutdown-weak-ref?
                   :mapdb/concurrency-scale
                   :mapdb/executor-enable?
                   :mapdb/transaction-enable?]))

(defmethod db-type :default [_]
  (s/keys :req-un [:mapdb/db-type]
          :opt-un [:mapdb/file-mmap-enable-if-supported?
                   :mapdb/read-only?
                   :mapdb/checksum-header-bypass?
                   :mapdb/checksum-store-enable?
                   :mapdb/cleaner-hack-enable?
                   :mapdb/close-on-jvm-shutdown?
                   :mapdb/close-on-jvm-shutdown-weak-ref?
                   :mapdb/concurrency-scale
                   :mapdb/executor-enable?
                   :mapdb/transaction-enable?]))

(s/def :mapdb/options (s/multi-spec db-type :db-type))

(s/fdef open-database!
  :args (s/cat :options (s/? :mapdb/options))
  :ret #(instance? DB %))

(defn open-database!
  ^DB
  ([{:keys [db-type] :as opts}]
   (let [builder (mapdb-types (keyword db-type))
         ^DBMaker$Maker dbmaker (builder opts)]
     (configure-maker! mapdb-options dbmaker opts)
     (.make dbmaker)))
  ([] (open-database! {:db-type :heap})))

(def ^:private serializers
  {:big-decimal        org.mapdb.Serializer/BIG_DECIMAL
   :big-integer        org.mapdb.Serializer/BIG_INTEGER
   :boolean            org.mapdb.Serializer/BOOLEAN
   :byte               org.mapdb.Serializer/BYTE
   :byte-array         org.mapdb.Serializer/BYTE_ARRAY
   :byte-array-delta   org.mapdb.Serializer/BYTE_ARRAY_DELTA
   :byte-array-nosize  org.mapdb.Serializer/BYTE_ARRAY_NOSIZE
   :char               org.mapdb.Serializer/CHAR
   :char-array         org.mapdb.Serializer/CHAR_ARRAY
   :class              org.mapdb.Serializer/CLASS
   :date               org.mapdb.Serializer/DATE
   :double             org.mapdb.Serializer/DOUBLE
   :double-array       org.mapdb.Serializer/DOUBLE_ARRAY
   :elsa               org.mapdb.Serializer/ELSA
   :float              org.mapdb.Serializer/FLOAT
   :float-array        org.mapdb.Serializer/FLOAT_ARRAY
   :illegal-access     org.mapdb.Serializer/ILLEGAL_ACCESS
   :integer            org.mapdb.Serializer/INTEGER
   :integer-delta      org.mapdb.Serializer/INTEGER_DELTA
   :integer-packed     org.mapdb.Serializer/INTEGER_PACKED
   :int-array          org.mapdb.Serializer/INT_ARRAY
   :java               org.mapdb.Serializer/JAVA ; NB java object serialization, the default
   :long               org.mapdb.Serializer/LONG
   :long-array         org.mapdb.Serializer/LONG_ARRAY
   :long-delta         org.mapdb.Serializer/LONG_DELTA
   :long-packed        org.mapdb.Serializer/LONG_PACKED
   :recid              org.mapdb.Serializer/RECID
   :recid-array        org.mapdb.Serializer/RECID_ARRAY
   :short              org.mapdb.Serializer/SHORT
   :short-array        org.mapdb.Serializer/SHORT_ARRAY
   :string             org.mapdb.Serializer/STRING
   :string-ascii       org.mapdb.Serializer/STRING_ASCII
   :string-delta       org.mapdb.Serializer/STRING_DELTA
   :string-intern      org.mapdb.Serializer/STRING_INTERN
   :string-nosize      org.mapdb.Serializer/STRING_NOSIZE
   :string-orighash    org.mapdb.Serializer/STRING_ORIGHASH
   :uuid               org.mapdb.Serializer/UUID})

(def ^:private composite-serializers
  {:edn {:raw-serializer :string
         :wrapper-serializer {:encoder (fn mapdb-edn-encoder [v] (pr-str v))
                              :decoder (fn mapdb-edn-decoder [v] (edn/read-string v))}}})

(sdef-enum :mapdb/standard-serializer-type (into #{} (keys serializers)))
(sdef-enum :mapdb/composite-serializer-type (into #{} (keys composite-serializers)))

(s/def :mapdb.coll/counter-enable? boolean?)

(s/def :mapdb.coll/raw-serializer (s/or :standard-serializer :mapdb/standard-serializer-type
                                        :native-serializer #(instance? Serializer %)))
(s/def :mapdb.coll/encoder (s/fspec :args (s/cat :arg any?)
                                    :ret any?))
(s/def :mapdb.coll/decoder (s/fspec :args (s/cat :arg any?)
                                    :ret any?))
(s/def :mapdb.coll/wrapper-serializer (s/keys :req-un [:mapdb.coll/encoder
                                                       :mapdb.coll/decoder]))
(s/def :mapdb.coll/composite-serializer (s/keys :req-un [:mapdb.coll/raw-serializer
                                                         :mapdb.coll/wrapper-serializer]))
(s/def :mapdb.coll/serializer (s/or :raw-serializer :mapdb.coll/raw-serializer
                                    :predefined-composite-serializer :mapdb/composite-serializer-type
                                    :composite-serializer :mapdb.coll/composite-serializer))

(s/def :mapdb.coll/value-serializer :mapdb.coll/serializer)
(s/def :mapdb.coll/key-serializer :mapdb.coll/serializer)

(s/def :mapdb.hashmap/concurrency int?)
(s/def :mapdb.hashmap/node-size int?)
(s/def :mapdb.hashmap/levels int?)
(s/def :mapdb.hashmap/layout (s/keys :req-un [:mapdb.hashmap/concurrency
                                              :mapdb.hashmap/node-size
                                              :mapdb.hashmap/levels]))
(s/def :mapdb.hashmap/hash-seed int?)

(s/def :mapdb.hashmap/options (s/keys :opt-un [:mapdb.coll/counter-enable?
                                               :mapdb.coll/value-serializer
                                               :mapdb.coll/key-serializer
                                               :mapdb.hashmap/layout
                                               :mapdb.hashmap/hash-seed]))

(def ^:private hashmap-options
  {:counter-enable? (boolean-setter counterEnable org.mapdb.DB$HashMapMaker)
   :value-serializer (fn [^DB$HashMapMaker dbm s]
                       (if (instance? Serializer s)
                         (.valueSerializer dbm s)
                         (.valueSerializer dbm (serializers s))))
   :key-serializer (fn [^DB$HashMapMaker dbm s]
                     (if (instance? Serializer s)
                       (.keySerializer dbm s)
                       (.keySerializer dbm (serializers s))))
   :layout (fn [^DB$HashMapMaker dbm
                {:keys [concurrency node-size levels]
                 :or {concurrency 8
                      node-size 16
                      levels 4}}]
             (.layout dbm concurrency node-size levels))
   :hash-seed (fn [^DB$HashMapMaker dbm seed]
                (.hashSeed dbm seed))})

(s/fdef open-raw-treemap!
  :args (s/cat :db #(instance? DB %)
               :hashmap-name (s/or :string-name string?
                                   :keyword-name keyword?)
               :options :mapdb.hashmap/options)
  :ret #(instance? HTreeMap %))

(defn open-raw-hashmap!
  [^DB db hashmap-name opts]
  (let [params (merge {:key-serializer :java
                       :value-serializer :java}
                      opts)
        hmap   (.hashMap db (name hashmap-name))]
    (configure-maker! hashmap-options hmap opts)
    (.createOrOpen hmap)))

(defprotocol PDatabase
  (make-db [it] "Returns a `org.mapdb.DB` from the given arg."))

(extend-protocol PDatabase
  DB
  (make-db [it] it)
  clojure.lang.IPersistentMap
  (make-db [opts] (open-database! opts))
  nil
  (make-db [_] (open-database!)))

(defprotocol PTransactionable
  (commit! [this] "Commits the changes to storage up to this point of time.")
  (rollback! [this] "Rollbacks all changes since the last commit."))

(defprotocol PCloseable
  (close [this] "Closes the underlying db. Necessary to avoid corruption if transaction are disabled."))

(defprotocol PMapDBBased
  (get-db [this])
  (get-db-options [this])
  (get-db-type [this])
  (get-collection [this])
  (get-collection-name [this])
  (get-collection-type [this])
  (get-collection-options [this])
  (get-wrapper-serializers [this])
  (compact! [this] "Compacts the underlying storage to reclaim space and performance lost to fragmentattion."))

(extend-protocol PCloseable
  DB
  (close [this] (.close this)) )

(extend-protocol PTransactionable
  DB
  (commit! [this] (.commit this))
  (rollback! [this] (.rollback this)))

(extend-protocol PMapDBBased
  DB
  (get-db [this] this)
  (compact! [this] (.. this getStore compact)))

(s/fdef update!
  :args (s/cat :map #(satisfies? PMapDBBased %)
               :key any?
               :fn fn?
               :optional-args (s/* any?))
  :ret #(satisfies? PMapDBBased %))

(defn update!
  "Updates atomically a value, where `k` is a
  key and `f` is a function that will take the old value
  and any supplied `args` and return the new value.
  If the key does not exist, _nil_ is passed as the old value.
  If the update cannot happen atomically, a `ConcurrentModificationException`
  is thrown."
  [m k f & args]
  (let [^java.util.concurrent.ConcurrentMap raw-m (get-collection m)
        {:keys [key-encoder value-decoder value-encoder]} (get-wrapper-serializers m)
        ke (key-encoder k)
        current-raw (.get raw-m ke)
        current (value-decoder current-raw)
        new-val (apply f current args)
        result (if (nil? current-raw)
                 (let [val-before-put (.putIfAbsent ^MapExtra raw-m ke (value-encoder new-val))]
                   (if (nil? val-before-put)
                     true false))
                 (.replace raw-m ke current-raw (value-encoder new-val)))]
    (if result
      m
      (throw (java.util.ConcurrentModificationException.
              (format "Could not update value in mapdb [%s] for key [%s] atomically - concurrent modification detected"
                      (get-collection-name m) (str k)))))))

(s/fdef update-in!
  :args (s/cat :map #(satisfies? PMapDBBased %)
               :keys (s/coll-of any?)
               :fn fn?
               :optional-args (s/* any?))
  :ret #(satisfies? PMapDBBased %))

(defn update-in!
  "Same as [[update!]] but with a path of keys."
  [m ks f & args]
  (let [[top & left] ks]
    (update! m top (fn [old] (update-in old left #(apply f % args))))))

(defn- merge-serializers
  [options]
  (reduce
   (fn [acc [label v]]
     (if-let [{:keys [raw-serializer wrapper-serializer]} (composite-serializers v)]
       (assoc acc
              label raw-serializer
              (keyword (str (name label) "-wrapper")) wrapper-serializer)
       acc))
   options (select-keys options [:key-serializer :value-serializer])))

(deftype CljHashMap [^DB db db-opts
                     hm-name ^HTreeMap hm hm-opts
                     key-encoder key-decoder
                     value-encoder value-decoder
                     metadata]
  clojure.lang.ITransientMap
  (assoc [this k v] (do (.put hm (key-encoder k) (value-encoder v)) this))
  (without [this k] (do (.remove hm (key-encoder k)) this))
  (valAt [_ k] (value-decoder (.get hm (key-encoder k))))
  (valAt [_ k default] (let [ke (key-encoder k)]
                         (if (.containsKey hm ke)
                           (value-decoder (.get hm ke))
                           default)))
  (conj [this [k v]] (do (.put hm (key-encoder k) (value-encoder v)) this))
  (count [_] (.sizeLong hm))
  clojure.lang.ITransientAssociative2
  (containsKey [_ k] (.containsKey hm (key-encoder k)))
  (entryAt [this k] (let [v (.valAt this k)]
                      (clojure.lang.MapEntry. k v)))
  clojure.lang.IMapIterable
  (keyIterator [_] (let [trx (map key-decoder)]
                     (.iterator ^Iterable (eduction trx (iterator-seq (.iterator (.keySet hm)))))))
  (valIterator [_] (let [trx (map value-decoder)]
                     (.iterator ^Iterable (eduction trx (iterator-seq (.iterator (.values hm)))))))
  clojure.lang.Seqable
  (seq [this] (when-not (.isEmpty hm)
                (let [trx (map (fn [^java.util.Map$Entry kv]
                                 (clojure.lang.MapEntry. (key-decoder (.getKey kv))
                                                         (value-decoder (.getValue kv)))))]
                  (sequence trx (iterator-seq (.. hm entrySet iterator))))))
  clojure.lang.IFn
  (invoke [this k] (.valAt this k))
  (invoke [this k default] (.valAt this k default))
  clojure.lang.IMeta
  (meta [_] @metadata)
  clojure.lang.IReference
  (alterMeta [_ f args] (reset! metadata (apply f @metadata args)))
  (resetMeta [_ m] (reset! metadata m))
  clojure.lang.IKVReduce
  (kvreduce [_ f init]
    (let [^java.util.Iterator iter (.. hm entrySet iterator)]
      (loop [ret init]
        (if (.hasNext iter)
          (let [^java.util.Map$Entry kv (.next iter)
                ret (f ret (key-decoder (.getKey kv)) (value-decoder (.getValue kv)))]
            (if (reduced? ret)
              @ret
              (recur ret)))
          ret))))
  clojure.lang.IReduceInit
  (reduce [_ f init]
    (let [^java.util.Iterator iter (.. hm entrySet iterator)]
      (loop [ret init]
        (if (.hasNext iter)
          (let [^java.util.Map$Entry kv (.next iter)
                ret (f ret (clojure.lang.MapEntry. (key-decoder (.getKey kv))
                                                   (value-decoder (.getValue kv))))]
            (if (reduced? ret)
              @ret
              (recur ret)))
          ret))))
  clojure.lang.IReduce
  (reduce [_ f]
    (let [^java.util.Iterator iter (.. hm entrySet iterator)]
      (loop [ret nil]
        (if (.hasNext iter)
          (let [^java.util.Map$Entry kv (.next iter)
                ret (f ret (clojure.lang.MapEntry. (key-decoder (.getKey kv))
                                                   (value-decoder (.getValue kv))))]
            (if (reduced? ret)
              @ret
              (recur ret)))
          ret))))
  clojure.lang.MapEquivalence
  PTransactionable
  (commit! [this] (.commit db) this)
  (rollback! [this] (.rollback db) this)
  PCloseable
  (close [_] (.close db))
  PMapDBBased
  (get-db ^DB [_] db)
  (get-db-options [_] db-opts)
  (get-db-type [_] (:db-type db-opts))
  (get-collection ^HTreeMap [_] hm)
  (get-collection-name [_] hm-name)
  (get-collection-type [_] :hash-map)
  (get-collection-options [_] hm-opts)
  (get-wrapper-serializers [_] {:key-encoder key-encoder
                                :key-decoder key-decoder
                                :value-encoder value-encoder
                                :value-decoder value-decoder})
  (compact! [this] (.. db getStore compact) this)
  clojure.lang.IDeref
  (deref [_] (let [^java.util.Iterator iter (.. hm entrySet iterator)]
               (loop [ret (transient {})]
                 (if (.hasNext iter)
                   (let [^java.util.Map$Entry kv (.next iter)]
                     (recur (assoc! ret (key-decoder (.getKey kv)) (value-decoder (.getValue kv)))))
                   (persistent! ret))))))

(deftype CljTreeMap [^DB db db-opts
                     tm-name ^BTreeMap tm tm-opts
                     key-encoder key-decoder
                     value-encoder value-decoder
                     metadata]
  clojure.lang.ITransientMap
  (assoc [this k v] (do (.put tm (key-encoder k) (value-encoder v)) this))
  (without [this k] (do (.remove tm (key-encoder k)) this))
  (valAt [_ k] (value-decoder (.get tm (key-encoder k))))
  (valAt [_ k default] (let [ke (key-encoder k)]
                         (if (.containsKey tm ke)
                           (value-decoder (.get tm ke))
                           default)))
  (conj [this [k v]] (do (.put tm (key-encoder k) (value-encoder v)) this))
  (count [_] (.sizeLong tm))
  clojure.lang.ITransientAssociative2
  (containsKey [_ k] (.containsKey tm (key-encoder k)))
  (entryAt [this k] (let [v (.valAt this k)]
                      (clojure.lang.MapEntry. k v)))
  clojure.lang.IMapIterable
  (keyIterator [_] (let [trx (map key-decoder)]
                     (.iterator ^Iterable (eduction trx (iterator-seq (.descendingKeyIterator tm))))))
  (valIterator [_] (let [trx (map value-decoder)]
                     (.iterator ^Iterable (eduction trx (iterator-seq (.descendingValueIterator tm))))))
  clojure.lang.Seqable
  (seq [this] (when-not (.isEmpty tm)
                (let [trx (map (fn [^java.util.Map$Entry kv]
                                 (clojure.lang.MapEntry. (key-decoder (.getKey kv))
                                                         (value-decoder (.getValue kv)))))]
                  (sequence trx (iterator-seq (.. tm descendingEntryIterator))))))
  clojure.lang.IFn
  (invoke [this k] (.valAt this k))
  (invoke [this k default] (.valAt this k default))
  clojure.lang.IMeta
  (meta [_] @metadata)
  clojure.lang.IReference
  (alterMeta [_ f args] (reset! metadata (apply f @metadata args)))
  (resetMeta [_ m] (reset! metadata m))
  clojure.lang.IKVReduce
  (kvreduce [_ f init]
    (let [^java.util.Iterator iter (.. tm descendingEntryIterator)]
      (loop [ret init]
        (if (.hasNext iter)
          (let [^java.util.Map$Entry kv (.next iter)
                ret (f ret (key-decoder (.getKey kv)) (value-decoder (.getValue kv)))]
            (if (reduced? ret)
              @ret
              (recur ret)))
          ret))))
  clojure.lang.IReduceInit
  (reduce [_ f init]
    (let [^java.util.Iterator iter (.. tm descendingEntryIterator)]
      (loop [ret init]
        (if (.hasNext iter)
          (let [^java.util.Map$Entry kv (.next iter)
                ret (f ret (clojure.lang.MapEntry. (key-decoder (.getKey kv))
                                                   (value-decoder (.getValue kv))))]
            (if (reduced? ret)
              @ret
              (recur ret)))
          ret))))
  clojure.lang.IReduce
  (reduce [_ f]
    (let [^java.util.Iterator iter (.. tm descendingEntryIterator)]
      (loop [ret nil]
        (if (.hasNext iter)
          (let [^java.util.Map$Entry kv (.next iter)
                ret (f ret (clojure.lang.MapEntry. (key-decoder (.getKey kv))
                                                   (value-decoder (.getValue kv))))]
            (if (reduced? ret)
              @ret
              (recur ret)))
          ret))))
  clojure.lang.MapEquivalence
  PTransactionable
  (commit! [this] (.commit db) this)
  (rollback! [this] (.rollback db) this)
  PCloseable
  (close [_] (.close db))
  PMapDBBased
  (get-db ^DB [_] db)
  (get-db-options [_] db-opts)
  (get-db-type [_] (:db-type db-opts))
  (get-collection ^HTreeMap [_] tm)
  (get-collection-name [_] tm-name)
  (get-collection-type [_] :tree-map)
  (get-collection-options [_] tm-opts)
  (get-wrapper-serializers [_] {:key-encoder key-encoder
                                :key-decoder key-decoder
                                :value-encoder value-encoder
                                :value-decoder value-decoder})
  (compact! [this] (.. db getStore compact) this)
  clojure.lang.IDeref
  (deref [_] (let [^java.util.Iterator iter (.. tm entryIterator)]
               (loop [ret (transient {})]
                 (if (.hasNext iter)
                   (let [^java.util.Map$Entry kv (.next iter)]
                     (recur (assoc! ret (key-decoder (.getKey kv)) (value-decoder (.getValue kv)))))
                   (persistent! ret))))))

(s/def :mapdb.treemap/values-outside-nodes-enable? boolean?)
(s/def :mapdb.treemap/max-node-size int?)
(s/def :mapdb.treemap/initial-state (s/coll-of #(instance? java.util.Map$Entry %)))
(s/def :mapdb.treemap/options (s/keys :opt-un [:mapdb.coll/counter-enable?
                                               :mapdb.coll/value-serializer
                                               :mapdb.coll/key-serializer
                                               :mapdb.treemap/values-outside-nodes-enable?
                                               :mapdb.treemap/max-node-size
                                               :mapdb.treemap/initial-state]))

(def ^:private treemap-options
  {:counter-enable? (boolean-setter counterEnable org.mapdb.DB$TreeMapMaker)
   :value-serializer (fn [^DB$TreeMapMaker dbm s]
                       (if (instance? Serializer s)
                         (.valueSerializer dbm s)
                         (.valueSerializer dbm (serializers s))))
   :key-serializer (fn [^DB$TreeMapMaker dbm s]
                     (if (instance? Serializer s)
                       (.keySerializer dbm s)
                       (.keySerializer dbm (serializers s))))
   :values-outside-nodes-enable? (boolean-setter valuesOutsideNodesEnable org.mapdb.DB$TreeMapMaker)
   :max-node-size (fn [^DB$TreeMapMaker dbm size]
                    (.maxNodeSize dbm size))})

(s/fdef open-raw-treemap!
  :args (s/cat :db #(instance? DB %)
               :treemap-name (s/or :string-name string?
                                   :keyword-name keyword?)
               :options :mapdb.treemap/options)
  :ret #(instance? BTreeMap %))

(defn open-raw-treemap!
  [^DB db treemap-name {:keys [initial-content] :as opts}]
  (let [params (merge {:key-serializer :java
                       :value-serializer :java}
                      opts)
        tmap   (.treeMap db (name treemap-name))]
    (configure-maker! treemap-options tmap opts)
    (if initial-content
      (.createFrom tmap (.iterator ^Iterable (map (fn [[k v]] (kotlin.Pair. k v))
                                                  (reverse initial-content))))
      (.createOrOpen tmap))))

(def ^:private collection-types
  {:hash-map {:raw-builder (fn [db nam opts] (open-raw-hashmap! db nam opts))
              :wrapper-builder (fn [db db-opts
                                    nam raw-collection collection-opts
                                    {:keys [key-encoder key-decoder
                                            value-encoder value-decoder]
                                     :or {key-encoder identity
                                          key-decoder identity
                                          value-encoder identity
                                          value-decoder identity}}
                                    metadata]
                                 (CljHashMap. db db-opts (name nam)
                                              raw-collection collection-opts key-encoder key-decoder
                                              value-encoder value-decoder
                                              (atom {::name (name nam)})))}
   :tree-map {:raw-builder (fn [db nam opts] (open-raw-treemap! db nam opts))
              :wrapper-builder (fn [db db-opts
                                    nam raw-collection collection-opts
                                    {:keys [key-encoder key-decoder
                                            value-encoder value-decoder]
                                     :or {key-encoder identity
                                          key-decoder identity
                                          value-encoder identity
                                          value-decoder identity}}
                                    metadata]
                                 (CljTreeMap. db db-opts (name nam)
                                              raw-collection collection-opts key-encoder key-decoder
                                              value-encoder value-decoder
                                              (atom {::name (name nam)})))}})

(sdef-enum :mapdb/collection-type (into #{} (keys collection-types)))
(s/def :mapdb/db-or-spec (s/or :db-spec :mapdb/options 
                               :db-instance #(instance? DB %)))

(s/fdef open-collection!
  :args (s/cat :db :mapdb/db-or-spec
               :collection-type :mapdb/collection-type
               :collection-name (s/or :string-name string?
                                      :keyword-name keyword?)
               :options (s/? (s/or :mapdb.hashmap/options
                                   :mapdb.treemap/options)))
  :ret #(satisfies? PMapDBBased %))

(defn open-collection!
  "Creates a datastructure backed by `db` with the given type, name and options.
  Ex: `(open-collections! {:db-type :file
                           :file   \"mydbfile\"
                           :transaction-enable? true
                           :close-on-jvm-shutdown? true}
                          :hash-map
                          \"myhashname\"
                          {:counter-enable? true})`"
  ([db-or-spec collection-type collection-name opts]
   (let [^DB db (make-db db-or-spec)
         {:keys [raw-builder wrapper-builder]} (collection-types collection-type)
         {:keys [key-serializer-wrapper value-serializer-wrapper] :as merged} (merge-serializers opts)
         raw-hm (raw-builder db collection-name merged)
         serializers (-> {}
                         (cond-> (:encoder key-serializer-wrapper) (assoc :key-encoder
                                                                          (:encoder key-serializer-wrapper)))
                         (cond-> (:decoder key-serializer-wrapper) (assoc :key-decoder
                                                                          (:decoder key-serializer-wrapper)))
                         (cond-> (:encoder value-serializer-wrapper) (assoc :value-encoder
                                                                            (:encoder value-serializer-wrapper)))
                         (cond-> (:decoder value-serializer-wrapper) (assoc :value-decoder
                                                                            (:decoder value-serializer-wrapper))))]
     (wrapper-builder db db-or-spec (name collection-name) raw-hm
                      (assoc merged :collection-type collection-type)
                      serializers (atom {::name (name collection-name)}))))
  ([db-or-spec collection-type collection-name] (open-collection! db-or-spec collection-type collection-name {:counter-enable? true})))
