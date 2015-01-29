(ns fixie.core
  (:require [clj-mapdb.core :as m]
            [potemkin [types :refer [definterface+ def-abstract-type deftype+]]]
            [clojure.string :as str]
            [fixie.serializers :refer :all])
  (:import [java.util Map]
           [java.util.concurrent ConcurrentMap]
           [fixie.serializers NippySerializer]))

(defn nippy-serializer
  []
  (NippySerializer.))

(defn nippy-btree-key-serializer
  []
  (org.mapdb.BTreeKeySerializer$BasicKeySerializer. (nippy-serializer)))

(definterface+ IMapDB
  (db        [this] "Returns the underlying db")
  (close!    [this] "Closes this db")
  (closed?   [this] "Returns whether this db is closed")
  (commit!   [this] "Commits all pending changes on this db")
  (rollback! [this] "Rollbacks all pending changes on this db")
  (compact!  [this] "Compacts this DB to reclaim space")
  (rename!   [this old-name new-name] "Renames this collection")
  (snapshot  [this] "Returns a read-only snapshot view of this db")
  (storage   [this] "Returns the storage type for this db")
  (options   [this] "Returns the options this db was created with"))

(def-abstract-type MapColl
  IMapDB
  (db        [this] (db (.mdb this)))
  (close!    [this] (close! (.mdb this)))
  (closed?   [this] (closed? (.mdb this)))
  (commit!   [this] (commit! (.mdb this)))
  (rollback! [this] (rollback! (.mdb this)))
  (compact!  [this] (compact! (.mdb this)))
  (storage   [this] (throw (UnsupportedOperationException.)))
  (options   [this] (throw (UnsupportedOperationException.)))
  clojure.lang.Counted
  (count [this] (.size ^Map (.coll this)))
  clojure.lang.ILookup
  (valAt [this k] (.get ^Map (.coll this) k))
  (valAt [this k default] (let [res (.get ^Map (.coll this) k)] (if (nil? res) default res)))
  clojure.lang.ITransientMap
  (assoc [this k v] (.put ^Map (.coll this) k v) this)
  (without [this k] (.remove ^Map (.coll this) k) this)
  clojure.lang.Seqable
  (seq [this] (seq (.coll this)))
  clojure.lang.IFn
  (invoke [this k] (.get ^Map (.coll this) k))
  (invoke [this k default] (let [res (.get ^Map (.coll this) k)] (if (nil? res) default res)))
  clojure.lang.Named
  (getName [this] (.label this))
  java.util.Map
  (get [this k] (.get ^Map (.coll this) k))
  (isEmpty [this] (.isEmpty ^Map (.coll this)))
  (size [this] (.size ^Map (.coll this)))
  (keySet [this] (.keySet ^Map (.coll this)))
  (put [this k v] (.put ^Map (.coll this) k v))
  (putAll [this arg] ^Map (.putAll this arg))
  (clear [this] (.clear ^Map (.coll this)))
  (remove [this k] (.remove ^Map (.coll this) k))
  (values [this] (.values ^Map (.coll this)))
  (entrySet [this] (.entrySet ^Map (.coll this)))
  (containsKey [this k] (.containsKey ^Map (.coll this) k))
  (containsValue [this v] (.containsValue ^Map (.coll this) v))
  java.util.concurrent.ConcurrentMap
  (putIfAbsent [this k v] (.putIfAbsent ^ConcurrentMap (.coll this) k v))
  (remove [this k old-val] (.remove ^ConcurrentMap (.coll this) k old-val))
  (replace [this k old-val new-val] (.replace ^ConcurrentMap (.coll this) k old-val new-val))
  (replace [this k val] (.replace ^ConcurrentMap (.coll this) k val)))

(deftype+ DBHashMap [mdb coll label]
  MapColl)

(deftype+ DBTreeMap [mdb coll label]
  MapColl)

(def kw->type {:hash-map {:wrapper ->DBHashMap
                          :default {:key-serializer   (nippy-serializer)
                                    :value-serializer (nippy-serializer)}}
               :tree-map {:wrapper ->DBTreeMap
                          :default {:key-serializer   (nippy-btree-key-serializer)
                                    :value-serializer (nippy-serializer)}}})

(def java->type {org.mapdb.HTreeMap ->DBHashMap
                 org.mapdb.BTreeMap ->DBTreeMap})

(deftype MapDB [^org.mapdb.DB db storage options]
  IMapDB
  (db        [this] db)
  (close!    [this] (.close db))
  (closed?   [this] (.isClosed db))
  (commit!   [this] (.commit db) this)
  (rollback! [this] (.rollback db) this)
  (compact!  [this] (.compact db) this)
  (storage   [this] storage)
  (options   [this] options)
  clojure.lang.Counted
  (count [this] (count (.getAll db)))
  clojure.lang.ILookup
  (valAt [this k] (if-let [coll (.get db (name k))]
                    (let [label (name k)
                          wrapper (java->type (type coll))]
                      (if wrapper
                        (wrapper this coll label)
                        coll))))
  (valAt [this k opts] (throw (UnsupportedOperationException.)))
  clojure.lang.ITransientMap
  (assoc [this k opts] (let [typ (:type opts)
                             label (name k)
                             {:keys [default]} (kw->type typ)]
                         (m/create-collection! db typ label (merge default opts))
                         this))
  (without [this k] (.delete db (name k)) this)
  clojure.lang.Seqable
  (seq [this] (seq (.getAll db)))
  clojure.lang.IFn
  (invoke [this k] (.valAt this k))
  (invoke [this k _] (throw (UnsupportedOperationException.))))

(deftype TransactionMapDB [tx-mkr storage options])

(defmethod print-method TransactionMapDB
  [^TransactionMapDB db ^java.io.Writer w]
  (.write w "#TransactionMapDB<")
  (.write w (name (or (.storage db) "")))
  (.write w ">"))

(defmethod print-method DBHashMap
  [^DBHashMap coll ^java.io.Writer w]
  (try
    (print-method (.coll coll) w)
    (catch IllegalAccessError e
      (.write w (format "<%s : %s closed>" (name coll) (type coll))))))

(defmethod print-method DBTreeMap
  [^DBTreeMap coll ^java.io.Writer w]
  (try
    (print-method (.coll coll) w)
    (catch IllegalAccessError e
      (.write w (format "<%s : %s closed>" (name coll) (type coll))))))

(defmethod print-method MapDB
  [^MapDB db ^java.io.Writer w]
  (.write w "#MapDB<")
  (.write w (name (or (.storage db) "")))
  (.write w ">{")
  (try
    (let [all-colls (for [[k v] db]
                      (str k " <" (last (str/split (str (type v)) #"\.")) ">"))]
      (.write w (str/join ", " all-colls)))
    (catch IllegalAccessError e
      (.write w "closed")))
  (.write w "}"))

(defn mapdb
  ([db-type arg opts]
   (let [db (m/create-db db-type arg opts)]
     (if (:fully-transactional? opts)
       (->TransactionMapDB db db-type opts)
       (->MapDB db db-type opts))))
  ([db-type other] (if (map? other) (mapdb db-type nil other) (mapdb db-type other {})))
  ([db-type] (mapdb db-type nil {}))
  ([] (mapdb :heap nil {})))

(defn atomic-update-in!
  ([^ConcurrentMap m [k & ks] f args]
   (let [top-val (get m k ::absent)]
     (if (= top-val ::absent)
       (let [res (apply f nil args)
             full-res (if (> (count ks) 0)
                        (assoc-in {} ks res)
                        res)]
         (if (= full-res top-val)
           true
           (let [ret (.putIfAbsent m k full-res)]
             (when (nil? ret)
               true))))
       (let [res (if ks
                   (apply update-in top-val ks f args)
                   (apply f top-val args))]
         (if (= res top-val)
           true
           (.replace m k top-val res)))))))

(defn update-in!
  [m ks f & args]
  (loop []
    (when (not (atomic-update-in! m ks f args))
      (recur)))
  m)


;; NOT WORKING YET - DO NOT TOUCH
(defmacro with-tx
  [[mdb tx-maker] & body]
  (let [mdb-symb (symbol mdb)]
    `(let [~mdb-symb (->MapDB (.makeTx (.tx-mkr ~tx-maker)) (.storage ~tx-maker) {})]
       (try
         (let [return# ~@body]
           (when-not (closed? ~mdb-symb)
             (commit! ~mdb-symb))
           return#)
         (catch Exception e#
           (when-not (closed? ~mdb-symb)
             (rollback! ~mdb-symb))
           (throw e#))))))
