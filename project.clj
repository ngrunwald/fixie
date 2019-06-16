(defproject fixie "0.2.0-SNAPSHOT"
  :description "Clojure persisted on disk datastructures based on MapDB"
  :url "https://github.com/ngrunwald/fixie"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.mapdb/mapdb "3.0.7"
                  :exclusions [org.jetbrains.kotlin/kotlin-stdlib
                               org.eclipse.collections/eclipse-collections-api
                               org.eclipse.collections/eclipse-collections
                               org.eclipse.collections/eclipse-collections-forkjoin
                               com.google.guava/guava]]

                 ;; use static versions to override mapdb ranges
                 [org.jetbrains.kotlin/kotlin-stdlib "1.3.31"]
                 [org.eclipse.collections/eclipse-collections-api "10.0.0.M3"]
                 [org.eclipse.collections/eclipse-collections "10.0.0.M3"]
                 [org.eclipse.collections/eclipse-collections-forkjoin "10.0.0.M3"]
                 [com.google.guava/guava "23.0"]]
  :profiles {:dev {:dependencies [[metosin/testit "0.4.0"]
                                  [orchestra "2019.02.06-1"]
                                  [expound "0.7.2"]
                                  [org.clojure/test.check "0.10.0-alpha3"]]}
             :kaocha {:dependencies [[lambdaisland/kaocha "0.0-418"]
                                     [lambdaisland/kaocha-cloverage "0.0-32"]]}}
  :aliases {"kaocha" ["with-profile" "+kaocha" "run" "-m" "kaocha.runner"]})
