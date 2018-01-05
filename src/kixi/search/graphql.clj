(ns kixi.search.graphql
  (:require [clojure.spec.alpha :as spec]
            [clojure.spec.gen.alpha :as spec-gen]
            [com.rpl.specter :as specter
             :refer [MAP-VALS MAP-KEYS ALL transform recursive-path if-path STOP]]))

(def graphql-ns-seperator "___")

(defn hyphen->double-underscore
  [x]
  (clojure.string/replace x "-" "__"))

(defn double-underscore->hyphen
  [x]
  (clojure.string/replace x "__" "-"))


(defn period->underscore
  [x]
  (clojure.string/replace x "." "_"))

(defn underscore->period
  [x]
  (clojure.string/replace x "_" "."))

(defn encode
  [x]
  (-> x
      hyphen->double-underscore
      period->underscore))

(defn decode
  [x]
  (-> x
      double-underscore->hyphen
      underscore->period))

(spec/fdef keyword->graphql-name
           :args (spec/cat :kw keyword?)
           :ret (spec/and simple-keyword?
                          #(->> %
                                name
                                (re-matches #"[_A-Za-z][_0-9A-Za-z]*"))))

(defn keyword->graphql-keyword
  [kw]
  (if (qualified-keyword? kw)
    (let [kw-namespace (namespace kw)
          kw-name (name kw)]
      (keyword (str (encode kw-namespace)
                    graphql-ns-seperator
                    (encode kw-name))))
    (-> kw
        name
        encode
        keyword)))

(defn qualified-graphql-keyword?
  [kw]
  (clojure.string/includes? (name kw) graphql-ns-seperator))

(defn graphql-keyword->keyword
  [kw]
  (if (qualified-graphql-keyword? kw)
    (let [[kw-namespace kw-name] (clojure.string/split (name kw) (re-pattern graphql-ns-seperator))]
      (keyword (decode kw-namespace)
               (decode kw-name)))
    (keyword (decode (name kw)))))

(def nested-map-keys
  (recursive-path
   [] p
   (specter/if-path map?
                    [(specter/multi-path [MAP-KEYS]
                                         [MAP-VALS p])]
                    STOP)))

(defn namespaced-map->graphql-map
  [namespaced-schema]
  (transform [nested-map-keys]
             keyword->graphql-keyword
             namespaced-schema))

(defn graphql-map->namespaced-map
  [namespaced-schema]
  (transform [nested-map-keys]
             graphql-keyword->keyword
             namespaced-schema))

(defn assoc-in-queries
  [query condition]
  (prn query)
  (transform [MAP-VALS]
             #(merge %
                     condition)
             query))
