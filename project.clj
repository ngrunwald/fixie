(defproject fixie "0.2.0-SNAPSHOT"
  :description "Clojure persisted on disk datastructures based on MapDB"
  :url "https://github.com/ngrunwald/fixie"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.mapdb/mapdb "3.0.7"]]
  :profiles {:dev {:dependencies [[metosin/testit "0.3.0"]
                                  [orchestra "2018.12.06-2"]
                                  [expound "0.7.1"]]}
             :kaocha {:dependencies [[lambdaisland/kaocha "0.0-319"]
                                     [lambdaisland/kaocha-cloverage "0.0-22"]]}}
  :aliases {"kaocha" ["with-profile" "+kaocha" "run" "-m" "kaocha.runner"]})
