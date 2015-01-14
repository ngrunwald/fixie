(ns fixie.core
  (:require [clj-mapdb.core :as m]
            [potemkin [types :refer [definterface+ def-abstract-type deftype+]]]
            [clojure.string :as str]
            [taoensso.nippy :as nippy :refer [freeze-to-out! thaw-from-in!]]))

(defn nippy-serializer
  []
  (reify
    org.mapdb.Serializer
    (serialize [this out obj]
      (freeze-to-out! out obj))
    (deserialize [this in available]
      (thaw-from-in! in))
    (fixedSize [this] -1)
    java.io.Serializable))

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
  clojure.lang.Counted
  (count [this] (.sizeLong (.coll this)))
  clojure.lang.ILookup
  (valAt [this k] (.get (.coll this) k))
  (valAt [this k default] (if-let [res (.get (.coll this) k)] (if (nil? res) default res)))
  clojure.lang.ITransientMap
  (assoc [this k v] (.put (.coll this) k v) this)
  (without [this k] (.remove (.coll this) k) this)
  clojure.lang.Seqable
  (seq [this]
       (seq (.coll this)))
  clojure.lang.IFn
  (invoke [this k]
          (.get (.coll this) k))
  (invoke [this k default]
          (.get (.coll this) k))
  clojure.lang.Named
  (getName [this] (.label this))
  java.util.Map
  (get [this k]
       (.get (.coll this) k))
  (isEmpty [this]
           (.isEmpty (.coll this)))
  (size [this]
        (.size (.coll this)))
  (keySet [this]
          (.keySet (.coll this)))
  (put [this k v]
       (.put (.coll this) k v))
  (putAll [this arg]
          (.putAll this arg))
  (clear [this]
         (.clear (.coll this)))
  (remove [this k]
          (.remove (.coll this) k))
  (values [this]
          (.values (.coll this)))
  (entrySet [this]
            (.entrySet (.coll this)))
  (containsKey [this k]
               (.containsKey (.coll this) k))
  (containsValue [this v]
                 (.containsValue (.coll this) v)))

(deftype+ DBHashMap [db coll label]
  MapColl)

(deftype+ DBTreeMap [db coll label]
  MapColl)

(def kw->type {:hash-map {:wrapper ->DBHashMap
                          :default {:key-serializer   (nippy-serializer)
                                    :value-serializer (nippy-serializer)}}
               :tree-map {:wrapper ->DBTreeMap
                          :default {:key-serializer   (nippy-btree-key-serializer)
                                    :value-serializer (nippy-serializer)}}})

(def java->type {org.mapdb.HTreeMap ->DBHashMap
                 org.mapdb.BTreeMap ->DBTreeMap})

(deftype MapDB [db storage options]
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
  (count [this]
    (count (.getAll db)))
  clojure.lang.ILookup
  (valAt [this k] (if-let [coll (.get db (name k))]
                    (let [label (name k)
                          wrapper (java->type (type coll))]
                      (if wrapper
                        (wrapper this coll label)
                        coll))))
  (valAt [this k opts] (if-let [coll (.valAt this k)]
                         coll
                         (let [typ (:type opts)
                               nam (name k)
                               {:keys [wrapper default]} (kw->type typ)
                               coll (m/create-collection! db typ nam (merge default opts))]
                           (if wrapper
                             (wrapper this coll nam)
                             coll))))
  clojure.lang.ITransientMap
  (assoc [this k opts] (let [typ (:type opts)
                             label (name k)
                             {:keys [default]} (kw->type typ)]
                         (m/create-collection! db typ label (merge default opts))
                         this))
  (without [this k] (.delete db (name k)) this)
  clojure.lang.Seqable
  (seq [this]
    (seq (.getAll db)))
  clojure.lang.IFn
  (invoke [this k]
    (.valAt this k)))

(defmethod print-method DBHashMap
  [coll ^java.io.Writer w]
  (print-method (.coll coll) w))

(defmethod print-method DBTreeMap
  [coll ^java.io.Writer w]
  (print-method (.coll coll) w))

(defmethod print-method MapDB
  [db ^java.io.Writer w]
  (.write w "#MapDB<")
  (.write w (name (storage db)))
  (.write w ">{")
  (let [all-colls (for [[k v] db]
                    (str k " <" (last (str/split (str (type v)) #"\.")) ">"))]
    (.write w (str/join ", " all-colls)))
  (.write w "}"))

(defn mapdb
  ([db-type arg opts]
   (let [db (m/create-db db-type arg opts)]
     (->MapDB db db-type opts)))
  ([db-type other] (if (map? other) (mapdb db-type nil other) (mapdb other {})))
  ([db-type] (mapdb db-type nil {}))
  ([] (mapdb :heap nil {})))
