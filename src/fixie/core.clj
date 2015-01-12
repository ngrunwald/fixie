(ns fixie.core
  (:require [clj-mapdb.core :as m]
            [potemkin [types :refer [definterface+]]]
            [clojure.string :as str]))

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

(deftype MapDB [db storage options]
  IMapDB
  (db        [this] db)
  (close!    [this] (.close db) this)
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
  (valAt [this k] (.get db (name k)))
  (valAt [this k opts] (m/create-collection! db (:type opts) (name k) opts))
  clojure.lang.ITransientMap
  (assoc [this k opts] (m/create-collection! db (:type opts) (name k) opts) this)
  (without [this k] (.delete db (name k)) this)
  clojure.lang.Seqable
  (seq [this]
    (seq (.getAll db)))
  clojure.lang.IFn
  (invoke [this k]
    (get this (name k))))

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
