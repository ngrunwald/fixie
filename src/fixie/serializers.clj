(ns fixie.serializers
  (:require [taoensso.nippy
             :as nippy
             :refer [freeze-to-out! thaw-from-in!]]))

(deftype NippySerializer []
  org.mapdb.Serializer
  (serialize [this out obj]
    (freeze-to-out! out obj))
  (deserialize [this in available]
    (thaw-from-in! in))
  (fixedSize [this] -1)
  java.io.Serializable)
