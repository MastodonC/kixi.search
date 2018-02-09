(ns kixi.search.metadata.query
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [com.stuartsierra.component :as component]
            [kixi.search.elasticsearch.client :as es]
            [taoensso.timbre :as timbre :refer [info]]))

(defprotocol Query
  (find-by-id- [this id groups])
  (find-by-query- [this query-map]))

(s/def ::query
  (let [ex #(ex-info "Use stubbed fn version." {:fn %})]
    (s/with-gen
      (partial satisfies? Query)
      #(gen/return (reify Query
                     (find-by-id- [this id groups] (throw (ex "id")))
                     (find-by-query- [this query-map] (throw (ex "query"))))))))

(defn find-by-id
  [impl id groups]
  (find-by-id- impl id groups))

(defn find-by-query
  [impl query-map]
  (find-by-query- impl query-map))

(def sfirst (comp second first))

(def index-name "kixi-search_metadata")
(def doc-type "metadata")

(defrecord ElasticSearch
    [communications protocol host port profile
     es-url profile-index]

  Query
  (find-by-id-
    [this id groups]
    (es/get-by-id profile-index doc-type es-url id groups))

  (find-by-query-
    [this query-map]
    (es/search-data profile-index
                    doc-type
                    es-url
                    query-map))

  component/Lifecycle
  (start [component]
    (if-not es-url
      (do
        (info "Starting File Metadata Query")
        (assoc component
               :es-url (str protocol "://" host ":" port)
               :profile-index (str profile "-" index-name)))
      component))
  (stop [component]
    (if es-url
      (do (info "Stopping File Metadata Query")
          (dissoc component :es-url))
      component)))
