(ns fixie.core
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s])
  (:import [org.mapdb DBMaker DBMaker$Maker DB DB$HashMapMaker
            HTreeMap DB$TreeMapMaker BTreeMap Serializer MapExtra
            MapModificationListener]
           [java.util.concurrent TimeUnit ScheduledExecutorService Executors]
           [kotlin.jvm.functions Function1]))

(defn- configure-maker!
  [opts-map dbm opts]
  (doseq [setter-name (keys opts)
          :let [setter (opts-map setter-name)]
          :when setter
          :let [with-options? (::with-options? (meta setter))
                arg (if with-options? opts (opts setter-name))]]
   (setter dbm arg)))

(defmacro ^:private boolean-setter
  [meth tag]
  (let [tagged-db-name (vary-meta (gensym "db") assoc :tag tag)]
    `(fn [~tagged-db-name v#]
       (when v#
         (. ~tagged-db-name ~meth))
       ~tagged-db-name)))

(defmacro ^:private sdef-enum
  [nam enum]  
  `(s/def ~nam ~(eval enum)))

(defprotocol PDatabase
  (make-db [it] "Returns a `org.mapdb.DB` from the given arg."))

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
  (compact! [this] "Compacts the underlying storage to reclaim space and performance lost to fragmentattion.")
  (empty! [this] [this notify-listener?] "Empties the contents of the collection and signals listener or not."))

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
(sdef-enum :mapdb/db-type (into #{} (map first mapdb-types)))

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
  {:big-decimal        Serializer/BIG_DECIMAL
   :big-integer        Serializer/BIG_INTEGER
   :boolean            Serializer/BOOLEAN
   :byte               Serializer/BYTE
   :byte-array         Serializer/BYTE_ARRAY
   :byte-array-delta   Serializer/BYTE_ARRAY_DELTA
   :byte-array-nosize  Serializer/BYTE_ARRAY_NOSIZE
   :char               Serializer/CHAR
   :char-array         Serializer/CHAR_ARRAY
   :class              Serializer/CLASS
   :date               Serializer/DATE
   :double             Serializer/DOUBLE
   :double-array       Serializer/DOUBLE_ARRAY
   :elsa               Serializer/ELSA
   :float              Serializer/FLOAT
   :float-array        Serializer/FLOAT_ARRAY
   :illegal-access     Serializer/ILLEGAL_ACCESS
   :integer            Serializer/INTEGER
   :integer-delta      Serializer/INTEGER_DELTA
   :integer-packed     Serializer/INTEGER_PACKED
   :int-array          Serializer/INT_ARRAY
   :java               Serializer/JAVA ; NB java object serialization, the default
   :long               Serializer/LONG
   :long-array         Serializer/LONG_ARRAY
   :long-delta         Serializer/LONG_DELTA
   :long-packed        Serializer/LONG_PACKED
   :recid              Serializer/RECID
   :recid-array        Serializer/RECID_ARRAY
   :short              Serializer/SHORT
   :short-array        Serializer/SHORT_ARRAY
   :string             Serializer/STRING
   :string-ascii       Serializer/STRING_ASCII
   :string-delta       Serializer/STRING_DELTA
   :string-intern      Serializer/STRING_INTERN
   :string-nosize      Serializer/STRING_NOSIZE
   :string-orighash    Serializer/STRING_ORIGHASH
   :uuid               Serializer/UUID})

(def ^:private ns-to-require (atom #{}))

(defmacro conditional-body
  [namespace & body]
  (let [maybe-ex (try (require [namespace]) (catch Exception e e))]
    (if (instance? Exception maybe-ex)
      `(fn [n#] (throw (ex-info (format "Could not find class or file for ns %s" ~(str namespace))
                                {:namespace ~(str namespace)})))
      (do
        (swap! ns-to-require conj namespace)
        `(do ~@body)))))

(doseq [n @ns-to-require]
  (require [n]))

(defn make-modification-listener
  [{:keys [value-serializer-wrapper key-serializer-wrapper]} f]
  (reify
    MapModificationListener
    (modify [this k old-val new-val triggered?]
      (let [key-decoder (:decoder key-serializer-wrapper)
            value-decoder (:decoder value-serializer-wrapper)]
        (f (key-decoder k) (value-decoder old-val) (value-decoder new-val) triggered?)))))

(defn make-value-loader
  [f]
  (reify
    Function1
    (invoke [thtois k] (f k))))

(def ^:private composite-serializers
  {:edn {:raw-serializer :string
         :wrapper-serializer {:encoder (fn mapdb-edn-encoder [v] (pr-str v))
                              :decoder (fn mapdb-edn-decoder [v] (edn/read-string v))}}
   :nippy {:raw-serializer :byte-array
           :wrapper-serializer {:encoder (conditional-body taoensso.nippy
                                                           (fn mapdb-nippy-encoder [v]
                                                             (taoensso.nippy/freeze v)))
                                :decoder (conditional-body taoensso.nippy
                                                           (fn mapdb-nippy-decoder [v]
                                                             (taoensso.nippy/thaw v)))}}
   :json {:raw-serializer :string
          :wrapper-serializer {:encoder (conditional-body cheshire.core
                                                           (fn mapdb-json-encoder [v]
                                                             (cheshire.core/encode v)))
                               :decoder (conditional-body cheshire.core
                                                          (fn mapdb-json-decoder [v]
                                                            (cheshire.core/decode v true)))}}})

(def time-units
  {:ms TimeUnit/MILLISECONDS
   :s TimeUnit/SECONDS
   :m TimeUnit/MINUTES
   :h TimeUnit/HOURS
   :d TimeUnit/DAYS})

(sdef-enum :mapdb/standard-serializer-type (into #{} (map first serializers)))
(sdef-enum :mapdb/composite-serializer-type (into #{} (map first composite-serializers)))
(sdef-enum :mapdb/time-unit (into #{} (map first time-units)))

(s/def :mapdb.coll/counter-enable? boolean?)

(s/def :mapdb.coll/raw-serializer (s/or :standard-serializer :mapdb/standard-serializer-type
                                        :native-serializer #(instance? Serializer %)))
(s/def :mapdb.coll/encoder any?)
(s/def :mapdb.coll/decoder any?)
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
(s/def :mapdb.hashmap/modification-listener fn?)
(s/def :mapdb.hashmap/expire-overflow #(instance? java.util.Map %))
(s/def :mapdb.hashmap/value-loader fn?)
(s/def :mapdb.hashmap/time-spec (s/or :duration-in-ms int?
                                      :duration-with-unit
                                      (s/cat :duration int?
                                             :unit (s/or :unit-kw :mapdb/time-unit
                                                         :unit-enum #(instance? TimeUnit %)))))
(s/def :mapdb.hashmap/expire-after-create (s/nilable :mapdb.hashmap/time-spec))
(s/def :mapdb.hashmap/expire-after-get (s/nilable :mapdb.hashmap/time-spec))
(s/def :mapdb.hashmap/expire-after-update (s/nilable :mapdb.hashmap/time-spec))
(s/def :mapdb.hashmap/full-time-spec (s/cat :duration int?
                                            :unit #(instance? TimeUnit %)))
(s/def :mapdb.hashmap/expire-compact-threshold float?)
(s/def :mapdb.hashmap/expire-executor (s/or :threads-count int?
                                            :instance #(instance? ScheduledExecutorService %)))
(s/def :mapdb.hashmap/expire-executor-period int?)
(s/def :mapdb.hashmap/expire-max-size int?)
(s/def :mapdb.hashmap/expire-store-size int?)

(s/def :mapdb.hashmap/options (s/keys :opt-un [:mapdb.coll/counter-enable?
                                               :mapdb.coll/value-serializer
                                               :mapdb.coll/key-serializer
                                               :mapdb.hashmap/layout
                                               :mapdb.hashmap/hash-seed
                                               :mapdb.hashmap/modification-listener
                                               :mapdb.hashmap/expire-overflow
                                               :mapdb.hashmap/value-loader
                                               :mapdb.hashmap/expire-after-create
                                               :mapdb.hashmap/expire-after-get
                                               :mapdb.hashmap/expire-after-update
                                               :mapdb.hashmap/expire-compact-threshold
                                               :mapdb.hashmap/expire-executor
                                               :mapdb.hashmap/expire-executor-period
                                               :mapdb.hashmap/expire-max-size
                                               :mapdb.hashmap/expire-store-size]))

(s/fdef to-time-spec
  :args (s/cat :time-spec :mapdb.hashmap/time-spec)
  :ret :mapdb.hashmap/full-time-spec)

(defn to-time-spec
  [v]
  (if (sequential? v)
    (let [unit (second v)]
      [(first v) (if (keyword? unit) (time-units unit) unit)])
    [(first v) (time-units :ms)]))

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
                (.hashSeed dbm seed))
   :modification-listener (vary-meta
                           (fn [^DB$HashMapMaker dbm {:keys [modification-listener] :as opts}]
                             (.modificationListener dbm
                                                    (make-modification-listener
                                                     (select-keys opts [:value-serializer-wrapper
                                                                        :key-serializer-wrapper])
                                                     modification-listener)))
                           assoc ::with-options? true)
   :expire-overflow (fn [^DB$HashMapMaker dbm m]
                      (.expireOverflow dbm m))
   :value-loader (fn [^DB$HashMapMaker dbm f]
                   (.valueLoader dbm (make-value-loader f)))
   :expire-after-create (fn [^DB$HashMapMaker dbm v]
                          (if (nil? v)
                            (.expireAfterCreate dbm)
                            (let [[duration unit] (to-time-spec v)]
                              (.expireAfterCreate dbm duration unit))))
   :expire-after-get (fn [^DB$HashMapMaker dbm v]
                       (if (nil? v)
                         (.expireAfterGet dbm)
                         (let [[duration unit] (to-time-spec v)] 
                          (.expireAfterGet dbm duration unit))))
   :expire-after-update (fn [^DB$HashMapMaker dbm v]
                          (if (nil? v)
                            (.expireAfterUpdate dbm)
                            (let [[duration unit] (to-time-spec v)] 
                             (.expireAfterUpdate dbm duration unit))))
   :expire-compact-threshold (fn [^DB$HashMapMaker dbm v]
                               (.expireOverflow dbm v))
   :expire-executor (fn [^DB$HashMapMaker dbm v]
                      (if (int? v)
                        (.expireExecutor dbm (Executors/newScheduledThreadPool v))
                        (.expireExecutor dbm v)))
   :expire-executor-period (fn [^DB$HashMapMaker dbm v]
                             (.expireExecutorPeriod dbm v))
   :expire-max-size (fn [^DB$HashMapMaker dbm v]
                      (.expireMaxSize dbm v))
   :expire-store-size (fn [^DB$HashMapMaker dbm v]
                      (.expireStoreSize dbm v))})

(s/fdef open-raw-treemap!
  :args (s/cat :db #(instance? DB %)
               :hashmap-name (s/or :string-name string?
                                   :keyword-name keyword?)
               :options :mapdb.hashmap/options)
  :ret #(instance? HTreeMap %))

(defn open-raw-hashmap!
  [^DB db hashmap-name opts]
  (let [params (merge {:key-serializer :java
                       :value-serializer :java
                       :key-serializer-wrapper {:encoder identity :decoder identity}
                       :value-serializer-wrapper {:encoder identity :decoder identity}}
                      opts)
        hmap   (.hashMap db (name hashmap-name))]
    (configure-maker! hashmap-options hmap params)
    (.createOrOpen hmap)))

(extend-protocol PDatabase
  DB
  (make-db [it] it)
  clojure.lang.IPersistentMap
  (make-db [opts] (open-database! opts))
  nil
  (make-db [_] (open-database!)))

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

(s/def :mapdb.coll/collection #(or (instance? clojure.lang.ITransientMap %)
                                   (instance? clojure.lang.ITransientSet %)))

(s/fdef into!
  :args (s/cat :to :mapdb.coll/collection
               :from coll?)
  :fn #(= (:ret %) (-> % :args :to))
  :ret :mapdb.coll/collection)

(defn into!
  "Merges given collection into the fixie datastructure."
  [to from]
  (let [adder (cond (instance? clojure.lang.ITransientMap to)
                    #(assoc! %1 (first %2) (second %2))

                    (instance? clojure.lang.ITransientSet to)
                    conj!

                    :else (throw (ex-info (format "Data type %s not recognized by into!" (type to))
                                          {:data-type (type to)})))]
    (doseq [x from]
      (adder to x))
    to))

(defn- merge-serializers
  [options]
  (reduce
   (fn [acc [label v]]
     (if-let [{:keys [raw-serializer wrapper-serializer]} (if (keyword? v)
                                                            (composite-serializers v)
                                                            v)]
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
  (empty! [this] (empty! this false))
  (empty! [this notify-listener?]
    (if notify-listener?
      (.clearWithExpire hm)
      (.clearWithoutNotification hm))
    this)
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
  (empty! [this] (.clear tm) this)
  (empty! [this _] (.clear tm) this)
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
                       :value-serializer :java
                       :key-serializer-wrapper {:encoder identity :decoder identity}
                       :value-serializer-wrapper {:encoder identity :decoder identity}}
                      opts)
        tmap   (.treeMap db (name treemap-name))]
    (configure-maker! treemap-options tmap params)
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
  :args (s/cat :db (s/? :mapdb/db-or-spec)
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
  ([db-or-spec collection-type collection-name] (open-collection! db-or-spec collection-type collection-name {:counter-enable? true}))
  ([collection-type collection-name] (open-collection! {:db-type :heap} collection-type collection-name {:counter-enable? true})))
