(ns kixi.search.elasticsearch.migrators.migrate-20180129
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.metadatastore :as md]
            [kixi.datastore.metadatastore.time :as mt]
            [kixi.datastore.metadatastore.license :as ml]
            [kixi.datastore.metadatastore.geography :as mg]
            [kixi.search.elasticsearch.client :as es]
            [taoensso.timbre :as timbre :refer [info]]))

(def index-name "kixi-search_metadata")
(def doc-type "metadata")

(def index-definition
  {::md/id es/string-stored-not_analyzed
   ::md/type es/string-stored-not_analyzed
   ::md/file-type es/string-stored-not_analyzed
   ::md/name es/string-autocomplete
   ::md/description es/string-analyzed
   ::md/logo es/string-stored-not_analyzed
   ::md/header es/boolean
   ::md/tags es/string-autocomplete
   ::md/provenance {:properties {::md/source es/string-stored-not_analyzed
                                 :kixi.user/id es/string-stored-not_analyzed
                                 ::md/parent-id es/string-stored-not_analyzed
                                 ::md/created es/timestamp}}
   ::md/sharing {:properties {::md/file-read es/string-stored-not_analyzed
                              ::md/meta-visible es/string-stored-not_analyzed
                              ::md/meta-read es/string-stored-not_analyzed
                              ::md/meta-update es/string-stored-not_analyzed
                              ::md/bundle-add es/string-stored-not_analyzed}}
   ::mt/temporal-coverage {:properties {::mt/from es/timestamp
                                        ::mt/to es/timestamp}}
   ::ml/license {:properties {::ml/usage es/string-analyzed
                              ::ml/type es/string-analyzed}}
   ;; we havn't really sorted out geo usage, so strings for now, but should move to es geo types
   ::mg/geography {:properties {::mg/type es/string-analyzed
                                ::mg/level es/string-analyzed}}
   ::md/source-created es/timestamp
   ::md/source-updated es/timestamp
   ::md/source es/string-analyzed
   ::md/maintainer es/string-stored-not_analyzed
   ::md/author es/string-analyzed
   ::md/size-bytes es/long})

(defn es-url
  [{:keys [protocol host port]}]
  (str protocol "://" host ":" port))

(defn up
  [{:keys [profile] :as db}]
  (es/create-index (es-url db)
                   (str profile "-" index-name)
                   doc-type
                   index-definition))

(defn down
  [db])
