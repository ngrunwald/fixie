(defproject fixie "0.1.0-SNAPSHOT"
  :description "Clojure persisted on disk datastructures based on MapDB"
  :url "https://github.com/ngrunwald/fixie"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [clj-mapdb "0.1.0-SNAPSHOT"]
                 [com.taoensso/nippy "2.7.1"]]
  :profiles {:dev {:dependencies [[expectations "2.0.9"]]
                   :plugins [[lein-expectations "0.0.7"]]}})
