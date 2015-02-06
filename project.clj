(defproject fixie "0.1.0-beta2-SNAPSHOT"
  :description "Clojure persisted on disk datastructures based on MapDB"
  :url "https://github.com/ngrunwald/fixie"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [clj-mapdb "0.1.0-beta2-SNAPSHOT"]
                 [com.taoensso/nippy "2.7.1"]]
  :profiles {:dev {:aot [fixie.serializers clj-mapdb.serializers]}})
