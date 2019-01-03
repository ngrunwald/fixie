(defproject fixie "0.2.0-SNAPSHOT"
  :description "Clojure persisted on disk datastructures based on MapDB"
  :url "https://github.com/ngrunwald/fixie"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.mapdb/mapdb "3.0.7"
                  :exclusions [org.jetbrains.kotlin/kotlin-stdlib
                               org.eclipse.collections/eclipse-collections-api
                               org.eclipse.collections/eclipse-collections
                               org.eclipse.collections/eclipse-collections-forkjoin
                               com.google.guava/guava]]

                 ;; use static versions to override mapdb ranges
                 [org.jetbrains.kotlin/kotlin-stdlib "1.2.71"]
                 [org.eclipse.collections/eclipse-collections-api "9.2.0"]
                 [org.eclipse.collections/eclipse-collections "9.2.0"]
                 [org.eclipse.collections/eclipse-collections-forkjoin "9.2.0"]
                 [com.google.guava/guava "19.0"]]
  :profiles {:dev {:dependencies [[metosin/testit "0.3.0"]
                                  [orchestra "2018.12.06-2"]
                                  [expound "0.7.2"]
                                  [org.clojure/test.check "0.10.0-alpha3"]]}
             :kaocha {:dependencies [[lambdaisland/kaocha "0.0-319"]
                                     [lambdaisland/kaocha-cloverage "0.0-22"]]}}
  :aliases {"kaocha" ["with-profile" "+kaocha" "run" "-m" "kaocha.runner"]})
