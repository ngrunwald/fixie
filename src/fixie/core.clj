(ns fixie.core
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s])
  (:import [org.mapdb DBMaker DBMaker$Maker DB DB$HashMapMaker
            HTreeMap DB$TreeMapMaker BTreeMap Serializer MapExtra
            MapModificationListener SortedTableMap SortedTableMap$Companion$Maker]
           [org.mapdb.volume MappedFileVol ByteArrayVol ByteBufferMemoryVol Volume]
           [java.util.concurrent TimeUnit ScheduledExecutorService Executors]
           [kotlin.jvm.functions Function1]
           [java.util Map]))

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

(defprotocol PMapDBTransient
  (get-collection-name [this])
  (get-collection-type [this])
  (compact! [this] "Compacts the underlying storage to reclaim space and performance lost to fragmentattion.")
  (empty! [this] [this notify-listener?] "Empties the contents of the collection and signals listener or not."))

(defprotocol PMapDBPersistent
  (get-db [this])
  (get-db-type [this])
  (get-db-options [this])
  (get-collection [this])
  (get-collection-options [this])
  (get-wrapper-serializers [this]))

(def ^:private mapdb-types
  {:file (fn [{:keys [path]}]
           (let [f (io/file path)]
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

(s/def :fixie.db/file-mmap-enable-if-supported? boolean?)
(s/def :fixie.db/read-only? boolean?)
(s/def :fixie.db/checksum-header-bypass? boolean?)
(s/def :fixie.db/checksum-store-enable? boolean?)
(s/def :fixie.db/cleaner-hack-enable? boolean?)
(s/def :fixie.db/close-on-jvm-shutdown? boolean?)
(s/def :fixie.db/close-on-jvm-shutdown-weak-ref? boolean?)
(s/def :fixie.db/concurrency-scale int?)
(s/def :fixie.db/executor-enable? boolean?)
(s/def :fixie.db/transaction-enable? boolean?)
(s/def :fixie.db/path (s/or :path string?
                         :file #(instance? java.io.File %)))
(sdef-enum :fixie.db/db-type (into #{} (map first mapdb-types)))

(defmulti db-type :db-type)

(defmethod db-type :file [_]
  (s/keys :req-un [:fixie.db/db-type
                   :fixie.db/path]
          :opt-un [:fixie.db/file-mmap-enable-if-supported?
                   :fixie.db/read-only?
                   :fixie.db/checksum-header-bypass?
                   :fixie.db/checksum-store-enable?
                   :fixie.db/cleaner-hack-enable?
                   :fixie.db/close-on-jvm-shutdown?
                   :fixie.db/close-on-jvm-shutdown-weak-ref?
                   :fixie.db/concurrency-scale
                   :fixie.db/executor-enable?
                   :fixie.db/transaction-enable?]))

(defmethod db-type :default [_]
  (s/keys :req-un [:fixie.db/db-type]
          :opt-un [:fixie.db/file-mmap-enable-if-supported?
                   :fixie.db/read-only?
                   :fixie.db/checksum-header-bypass?
                   :fixie.db/checksum-store-enable?
                   :fixie.db/cleaner-hack-enable?
                   :fixie.db/close-on-jvm-shutdown?
                   :fixie.db/close-on-jvm-shutdown-weak-ref?
                   :fixie.db/concurrency-scale
                   :fixie.db/executor-enable?
                   :fixie.db/transaction-enable?]))

(s/def :fixie.db/options (s/multi-spec db-type :db-type))

(s/fdef open-database!
  :args (s/cat :options (s/? :fixie.db/options))
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
   :keyword {:raw-serializer :string
             :wrapper-serializer {:encoder name
                                  :decoder keyword}}
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

(sdef-enum :fixie/standard-serializer-type (into #{} (map first serializers)))
(sdef-enum :fixie/composite-serializer-type (into #{} (map first composite-serializers)))
(sdef-enum :fixie/time-unit (into #{} (map first time-units)))

(s/def :fixie.coll/counter-enable? boolean?)

(s/def :fixie.coll/raw-serializer (s/or :standard-serializer :fixie/standard-serializer-type
                                        :native-serializer #(instance? Serializer %)))
(s/def :fixie.coll/encoder any?)
(s/def :fixie.coll/decoder any?)
(s/def :fixie.coll/wrapper-serializer (s/keys :req-un [:fixie.coll/encoder
                                                       :fixie.coll/decoder]))
(s/def :fixie.coll/composite-serializer (s/keys :req-un [:fixie.coll/raw-serializer
                                                         :fixie.coll/wrapper-serializer]))
(s/def :fixie.coll/serializer (s/or :raw-serializer :fixie.coll/raw-serializer
                                    :predefined-composite-serializer :fixie/composite-serializer-type
                                    :composite-serializer :fixie.coll/composite-serializer))

(s/def :fixie.coll/value-serializer :fixie.coll/serializer)
(s/def :fixie.coll/key-serializer :fixie.coll/serializer)

(s/def :fixie.hashmap/concurrency int?)
(s/def :fixie.hashmap/node-size int?)
(s/def :fixie.hashmap/levels int?)
(s/def :fixie.hashmap/layout (s/keys :req-un [:fixie.hashmap/concurrency
                                              :fixie.hashmap/node-size
                                              :fixie.hashmap/levels]))
(s/def :fixie.hashmap/hash-seed int?)
(s/def :fixie.hashmap/modification-listener fn?)
(s/def :fixie.hashmap/expire-overflow #(instance? java.util.Map %))
(s/def :fixie.hashmap/value-loader fn?)
(s/def :fixie.hashmap/time-spec (s/or :duration-in-ms int?
                                      :duration-with-unit
                                      (s/cat :duration int?
                                             :unit (s/or :unit-kw :fixie/time-unit
                                                         :unit-enum #(instance? TimeUnit %)))))
(s/def :fixie.hashmap/expire-after-create (s/nilable :fixie.hashmap/time-spec))
(s/def :fixie.hashmap/expire-after-get (s/nilable :fixie.hashmap/time-spec))
(s/def :fixie.hashmap/expire-after-update (s/nilable :fixie.hashmap/time-spec))
(s/def :fixie.hashmap/full-time-spec (s/cat :duration int?
                                            :unit #(instance? TimeUnit %)))
(s/def :fixie.hashmap/expire-compact-threshold float?)
(s/def :fixie.hashmap/expire-executor (s/or :threads-count int?
                                            :instance #(instance? ScheduledExecutorService %)))
(s/def :fixie.hashmap/expire-executor-period int?)
(s/def :fixie.hashmap/expire-max-size int?)
(s/def :fixie.hashmap/expire-store-size int?)

(s/def :fixie.hashmap/options (s/keys :opt-un [:fixie.coll/counter-enable?
                                               :fixie.coll/value-serializer
                                               :fixie.coll/key-serializer
                                               :fixie.hashmap/layout
                                               :fixie.hashmap/hash-seed
                                               :fixie.hashmap/modification-listener
                                               :fixie.hashmap/expire-overflow
                                               :fixie.hashmap/value-loader
                                               :fixie.hashmap/expire-after-create
                                               :fixie.hashmap/expire-after-get
                                               :fixie.hashmap/expire-after-update
                                               :fixie.hashmap/expire-compact-threshold
                                               :fixie.hashmap/expire-executor
                                               :fixie.hashmap/expire-executor-period
                                               :fixie.hashmap/expire-max-size
                                               :fixie.hashmap/expire-store-size]))

(s/fdef to-time-spec
  :args (s/cat :time-spec :fixie.hashmap/time-spec)
  :ret :fixie.hashmap/full-time-spec)

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

(s/fdef open-raw-hashmap!
  :args (s/cat :db #(instance? DB %)
               :hashmap-name (s/or :string-name string?
                                   :keyword-name keyword?)
               :options :fixie.hashmap/options)
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

(extend-protocol PTransactionable
  DB
  (commit! [this] (.commit this))
  (rollback! [this] (.rollback this)))

(s/fdef update!
  :args (s/cat :map #(satisfies? PMapDBPersistent %)
               :key any?
               :fn fn?
               :optional-args (s/* any?))
  :ret #(satisfies? PMapDBPersistent %))

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
  :args (s/cat :map #(satisfies? PMapDBPersistent %)
               :keys (s/coll-of any?)
               :fn fn?
               :optional-args (s/* any?))
  :ret #(satisfies? PMapDBPersistent %))

(defn update-in!
  "Same as [[update!]] but with a path of keys."
  [m ks f & args]
  (let [[top & left] ks]
    (update! m top (fn [old] (update-in old left #(apply f % args))))))

(defn close!
  [^java.io.Closeable m]
  (.close m))

(s/def :fixie.coll/collection #(or (instance? clojure.lang.ITransientMap %)
                                   (instance? clojure.lang.ITransientSet %)))

(s/fdef into!
  :args (s/cat :to :fixie.coll/collection
               :from coll?)
  :fn #(= (:ret %) (-> % :args :to))
  :ret :fixie.coll/collection)

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
  java.io.Closeable
  (close [_] (.close db))
  Iterable
  (iterator [_] (let [trx (map (fn [^java.util.Map$Entry entry]
                                 (clojure.lang.MapEntry. (key-decoder (.getKey entry))
                                                         (value-decoder (.getValue entry)))))]
                  (.iterator ^Iterable (eduction trx (iterator-seq (.. hm entrySet iterator))))))
  PMapDBTransient
  (get-db ^DB [_] db)
  (get-db-options [_] db-opts)
  (get-db-type [_] (:db-type db-opts))
  (get-collection ^HTreeMap [_] hm)
  (get-collection-options [_] hm-opts)
  (get-wrapper-serializers [_] {:key-encoder key-encoder
                                :key-decoder key-decoder
                                :value-encoder value-encoder
                                :value-decoder value-decoder})
  PMapDBPersistent
  (get-collection-name [_] hm-name)
  (get-collection-type [_] :hash-map)
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
  Iterable
  (iterator [_] (let [trx (map (fn [^java.util.AbstractMap$SimpleImmutableEntry entry]
                                 (clojure.lang.MapEntry. (key-decoder (.getKey entry))
                                                         (value-decoder (.getValue entry)))))]
                  (.iterator ^Iterable (eduction trx (iterator-seq (.entryIterator tm))))))
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
  java.io.Closeable
  (close [_] (.close db))
  PMapDBTransient
  (get-db ^DB [_] db)
  (get-db-options [_] db-opts)
  (get-db-type [_] (:db-type db-opts))
  (get-collection ^BTreeMap [_] tm)
  (get-collection-options [_] tm-opts)
  (get-wrapper-serializers [_] {:key-encoder key-encoder
                                :key-decoder key-decoder
                                :value-encoder value-encoder
                                :value-decoder value-decoder})
  (empty! [this] (.clear tm) this)
  (empty! [this _]
    (throw (UnsupportedOperationException. "SortedTableMap is immutable.")))
  PMapDBPersistent
  (get-collection-name [_] tm-name)
  (get-collection-type [_] :tree-map)
  (compact! [this] (.. db getStore compact) this)
  clojure.lang.IDeref
  (deref [_] (let [^java.util.Iterator iter (.. tm entryIterator)]
               (loop [ret (transient {})]
                 (if (.hasNext iter)
                   (let [^java.util.Map$Entry kv (.next iter)]
                     (recur (assoc! ret (key-decoder (.getKey kv)) (value-decoder (.getValue kv)))))
                   (persistent! ret))))))

(s/def :fixie.treemap/values-outside-nodes-enable? boolean?)
(s/def :fixie.treemap/max-node-size int?)
(s/def :fixie.treemap/initial-content any?)
(s/def :fixie.treemap/options (s/keys :opt-un [:fixie.coll/counter-enable?
                                               :fixie.coll/value-serializer
                                               :fixie.coll/key-serializer
                                               :fixie.treemap/values-outside-nodes-enable?
                                               :fixie.treemap/max-node-size
                                               :fixie.treemap/initial-content]))

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
               :options :fixie.treemap/options)
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

(sdef-enum :fixie.coll/collection-type (into #{} (keys collection-types)))
(s/def :fixie.db/db-or-spec (s/or :db-spec :fixie.db/options 
                               :db-instance #(instance? DB %)))

(s/fdef open-collection!
  :args (s/cat :db (s/? :fixie.db/db-or-spec)
               :collection-type :fixie.coll/collection-type
               :collection-name (s/or :string-name string?
                                      :keyword-name keyword?)
               :options (s/? (s/or :fixie.hashmap/options
                                   :fixie.treemap/options)))
  :ret #(satisfies? PMapDBTransient %))

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
                      (-> merged
                          (assoc :collection-type collection-type)
                          (dissoc :initial-content))
                      serializers (atom {::name (name collection-name)}))))
  ([db-or-spec collection-type collection-name] (open-collection! db-or-spec collection-type collection-name {:counter-enable? true}))
  ([collection-type collection-name] (open-collection! {:db-type :heap} collection-type collection-name {:counter-enable? true})))

(deftype CljSortedTableMap [^Volume volume volume-opts
                            ^SortedTableMap stm stm-opts
                            key-encoder key-decoder
                            value-encoder value-decoder
                            metadata]
  clojure.lang.Associative
  (assoc [_ _ _] (throw (UnsupportedOperationException. "SortedTableMap is immutable.")))
  (entryAt [_ k] (let [v (.get stm (key-encoder k))]
                   (if v
                     (clojure.lang.MapEntry. k (value-decoder v))
                     nil)))
  (containsKey [_ k] (.containsKey stm (key-encoder k)))
  (valAt [_ k] (value-decoder (.get stm (key-encoder k))))
  (valAt [_ k default] (let [ke (key-encoder k)]
                         (if (.containsKey stm ke)
                           (value-decoder (.get stm ke))
                           default)))
  (seq [this] (when-not (.isEmpty stm)
                (let [trx (map (fn [^java.util.Map$Entry kv]
                                 (clojure.lang.MapEntry. (key-decoder (.getKey kv))
                                                         (value-decoder (.getValue kv)))))]
                  (sequence trx (iterator-seq (.. stm descendingEntryIterator))))))
  (empty [_] (throw (UnsupportedOperationException. "SortedTableMap is immutable.")))
  (equiv [this o] (and (instance? java.util.Map o)
                       (let [^java.util.Map other-map o]
                         (and (= (.size other-map) (.size stm))
                              (every? (fn [^clojure.lang.MapEntry entry]
                                        (= (.get other-map (key entry)) (val entry)))
                                      (.entrySet this))))))
  (cons [_ _] (throw (UnsupportedOperationException. "SortedTableMap is immutable.")))
  (count [_] (.sizeLong stm))
  clojure.lang.IMapIterable
  (keyIterator [_] (let [trx (map key-decoder)]
                     (.iterator ^Iterable (eduction trx (iterator-seq (.descendingKeyIterator stm))))))
  (valIterator [_] (let [trx (map value-decoder)]
                     (.iterator ^Iterable (eduction trx (iterator-seq (.descendingValueIterator stm))))))
  Iterable
  (iterator [_] (let [trx (map (fn [^java.util.AbstractMap$SimpleImmutableEntry entry]
                                 (clojure.lang.MapEntry. (key-decoder (.getKey entry))
                                                         (value-decoder (.getValue entry)))))]
                  (.iterator ^Iterable (eduction trx (iterator-seq (.entryIterator stm))))))
  java.util.Map
  (clear [this] (throw (UnsupportedOperationException. "SortedTableMap is immutable.")))
  (containsValue [_ k] (.containsValue stm (value-encoder k)))
  (entrySet [_] (let [entries (.entrySet stm)
                      trx (map (fn [^java.util.Map$Entry entry]
                                 (clojure.lang.MapEntry.
                                  (key-encoder (.getKey entry))
                                  (value-encoder (.getValue entry)))))]
                  (into #{} trx entries)))
  (equals [this o] (identical? this o))
  (get [this k] (.get stm (key-encoder k)))
  (hashCode [this] (hash-ordered-coll this))
  (isEmpty [this] (.isEmpty stm))
  (keySet [this] (let [ks (.keySet stm)
                       trx (map key-decoder)]
                   (into #{} trx ks)))
  (put [_ _ _] (throw (UnsupportedOperationException. "SortedTableMap is immutable.")))
  (putAll [_ _] (throw (UnsupportedOperationException. "SortedTableMap is immutable.")))
  (remove [_ _] (throw (UnsupportedOperationException. "SortedTableMap is immutable.")))
  (size [_] (.size stm))
  (values [_] (let [values (.getValues stm)]
                (map value-decoder values)))
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
    (let [^java.util.Iterator iter (.. stm descendingEntryIterator)]
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
    (let [^java.util.Iterator iter (.. stm descendingEntryIterator)]
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
    (let [^java.util.Iterator iter (.. stm descendingEntryIterator)]
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
  java.io.Closeable
  (close [_] (.close stm))
  PMapDBPersistent
  (get-db ^Volume [this] volume)
  (get-db-options [this] volume-opts)
  (get-collection ^SortedTableMap [this] stm)
  (get-collection-options [this] stm-opts)
  (get-wrapper-serializers [this] {:key-encoder key-encoder
                                   :key-decoder key-decoder
                                   :value-encoder value-encoder
                                   :value-decoder value-decoder})
  clojure.lang.IDeref
  (deref [_] (let [^java.util.Iterator iter (.. stm entryIterator)]
               (loop [ret (transient {})]
                 (if (.hasNext iter)
                   (let [^java.util.Map$Entry kv (.next iter)]
                     (recur (assoc! ret (key-decoder (.getKey kv)) (value-decoder (.getValue kv)))))
                   (persistent! ret))))))

(def volume-types
  {:file (fn [{:keys [path]}]
           (-> (MappedFileVol/FACTORY) (.makeVolume path false)))
   :read-only-file (fn [{:keys [path]}]
                     (-> (MappedFileVol/FACTORY) (.makeVolume path true)))
   :memory (fn [{:keys [_]}]
             (-> (ByteArrayVol/FACTORY) (.makeVolume nil false)))
   :direct-memory (fn [{:keys [_]}]
                    (-> (ByteBufferMemoryVol/FACTORY) (.makeVolume nil false)))})

(def ^:private sorted-table-map-options
  {:page-size (fn [^org.mapdb.SortedTableMap$Companion$Maker stm size]
                (.pageSize stm size))
   :node-size (fn [^org.mapdb.SortedTableMap$Companion$Maker stm size]
                (.nodeSize stm size))})

(sdef-enum :fixie.volume/volume-type (into #{} (keys volume-types)))
(s/def :fixie.volume/path string?)

(s/def :fixie.volume/options (s/keys :req-un [:fixie.volume/volume-type]
                                     :opt-un [:fixie.volume/path]))

(s/def :fixie.sorted-table-map/node-size int?)
(s/def :fixie.sorted-table-map/page-size int?)
(s/def :fixie.sorted-table-map/content any?)
(s/def :fixie.volume/volume #(instance? Volume %))
(s/def :fixie.volume/volume-or-spec (s/or :volume :fixie.volume/volume
                                          :spec :fixie.volume/options))

(s/def :fixie.sorted-table-map/options (s/keys :opt-un [:fixie.sorted-table-map/content
                                                        :fixie.sorted-table-map/node-size
                                                        :fixie.sorted-table-map/page-size
                                                        :fixie.coll/key-serializer
                                                        :fixie.coll/value-serializer]))

(s/fdef open-sorted-table-map!
  :args (s/cat :volume :fixie.volume/volume-or-spec
               :options (s/? :fixie.sorted-table-map/options))
  :ret #(satisfies? PMapDBPersistent %))

(defn open-sorted-table-map!
  ([{:keys [volume-type] :as vol-opts}
    {original-value-serializer :value-serializer
     original-key-serializer :key-serializer
     :keys [content]
     :or {original-key-serializer :java original-value-serializer :java}
     :as opts}]
   (let [vol ((volume-types volume-type) vol-opts)
         {:keys [key-serializer-wrapper value-serializer-wrapper
                 key-serializer value-serializer]} (merge-serializers opts)
         {:keys [key-encoder value-encoder
                 key-decoder value-decoder]
          :or {key-encoder identity value-encoder identity
               key-decoder identity value-decoder identity}}
         (-> {}
             (cond-> (:encoder key-serializer-wrapper)
               (assoc :key-encoder
                      (:encoder key-serializer-wrapper)))
             (cond-> (:decoder key-serializer-wrapper)
               (assoc :key-decoder
                      (:decoder key-serializer-wrapper)))
             (cond-> (:encoder value-serializer-wrapper)
               (assoc :value-encoder
                      (:encoder value-serializer-wrapper)))
             (cond-> (:decoder value-serializer-wrapper)
               (assoc :value-decoder
                      (:decoder value-serializer-wrapper))))]
     (let [stm (if (not= volume-type :read-only-file)
                 (let [stm-build (SortedTableMap/create vol
                                                        (if (instance? Serializer key-serializer)
                                                          key-serializer
                                                          (if key-serializer
                                                            (serializers key-serializer)
                                                            (serializers original-key-serializer)))
                                                        (if (instance? Serializer value-serializer)
                                                          value-serializer
                                                          (if value-serializer
                                                            (serializers value-serializer)
                                                            (serializers original-value-serializer))))]
                   (configure-maker! sorted-table-map-options stm-build opts)
                   (let [sink (.createFromSink stm-build)]
                     (doseq [[k v] (sort-by first content)]
                       (.put sink (key-encoder k) (value-encoder v)))
                     (.create sink)))
                 (SortedTableMap/open vol
                                      (if (instance? Serializer key-serializer)
                                        key-serializer
                                        (serializers key-serializer))
                                      (if (instance? Serializer value-serializer)
                                        value-serializer
                                        (serializers value-serializer))))]
       (->CljSortedTableMap vol vol-opts stm
                            (-> opts
                                (dissoc :content)
                                (assoc :key-serializer original-key-serializer
                                       :value-serializer original-value-serializer))
                            key-encoder key-decoder value-encoder
                            value-decoder (atom vol-opts)))))
  ([volume-opts] (open-sorted-table-map! volume-opts {})))

(def ^:private pr-on #'clojure.core/pr-on)
(def ^:private print-meta #'clojure.core/print-meta)
(def ^:private print-map #'clojure.core/print-map)

(defmethod print-method CljSortedTableMap [m, ^java.io.Writer w]
  (print-meta m w)
  (print-map m pr-on w))
