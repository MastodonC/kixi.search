(ns kixi.search.elasticsearch.migrators.migrate-20180129
  (:require [com.stuartsierra.component :as component]
            [kixi.datastore.metadatastore :as md]
            [kixi.search.elasticsearch.client :as es]
            [taoensso.timbre :as timbre :refer [info]]))

(def index-name "kixi-datastore_file-metadata")
(def doc-type "file-metadata")

;;TODO this needs to be complete
(def index-definition
  {::md/id es/string-stored-not_analyzed
   ::md/type es/string-stored-not_analyzed
   ::md/file-type es/string-stored-not_analyzed
   ::md/name es/string-autocomplete
   ::md/description es/string-analyzed
   ::md/tags es/string-autocomplete
   ::md/provenance {:properties {::md/source es/string-stored-not_analyzed
                                 :kixi.user/id es/string-stored-not_analyzed
                                 ::md/parent-id es/string-stored-not_analyzed
                                 ::md/created es/timestamp}}
   ::md/sharing {:properties (zipmap md/activities
                                     (repeat es/string-stored-not_analyzed))}
   ::md/size-bytes es/long})

(defn es-url
  [{:keys [protocol host port]}]
  (str protocol "://" host ":" port))

(defn up
  [db]
  (es/create-index (es-url db)
                   index-name
                   doc-type
                   index-definition))

(defn down
  [db])
