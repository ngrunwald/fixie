[![Build Status](https://travis-ci.org/ngrunwald/fixie.svg)](https://travis-ci.org/ngrunwald/fixie)

# fixie

Clojure persisted on disk datastructures based on [MapDB](http://www.mapdb.org/).

## Usage

The library proposes for now two flavours of maps, backed by memory or disk, as transient structures.
For more details about the different options, have a look at [MapDB Docs](https://jankotek.gitbooks.io/mapdb/content/), the tests and the specs.

```clojure
(require '[fixie.core :as f])
(with-open [hm (f/open-collections! {:db-type :file
	                                 :file "mydbfile"
									 :transaction-enable? true
									 :close-on-jvm-shutdown? true}
									 :hash-map
									 "myhashname"
									 {:counter-enable? true
									  :key-serializer :edn
									  :value-serializer :int})]
	(assoc! hm :foo 42 :bar 54)  ;; {:foo 42 :bar 54}
	@hm ;; {:foo 42 :bar 54} => deref returns a PersistentMap
	(hm :foo) ;; 42
	(dissoc! hm :bar) ;; {:foo 42}
    (f/update! hm :foo inc) ;; {:foo 43} => atomic update with fixie.core/update
                            ;; and fixie.core/update-in
	(rollback! hm) ;; {} everything rolledback
	(assoc! hm [:composite :key] 56) ;; {[:composite :key] 56}
	(commit! hm) ;; => changes are persisted to disk/memory
	)
```

Fixie's heart is the `open-collection` fn which creates instances of the different data structures. First argument is either a `DB` object (created with `open-database!` for example) or a specification for DB with the same form. Mostly you should set `:db-type` and the path under `:file` if you chose `file` for `:db-type` (if you want persistence on disk).



## License

Copyright © 2018, 2019 Nils Grunwald

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
