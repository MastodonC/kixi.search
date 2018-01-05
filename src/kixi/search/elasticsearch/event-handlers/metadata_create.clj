(ns kixi.search.elasticsearch.event-handlers.metadata-create
  (:require [clojurewerkz.elastisch.rest.index :as esi]
            [com.stuartsierra.component :as component]
            [kixi.search.elasticsearch :as es]
            [kixi.spec :refer [alias]]
            [joplin.elasticsearch.database :refer [client]]))

(alias 'ms 'kixi.datastore.metadatastore)


(def index-name "kixi-datastore_file-metadata")
(def doc-type "file-metadata")

(def doc-def
  {::ms/id es/string-stored-not_analyzed
   ::ms/type es/string-stored-not_analyzed
   ::ms/name es/string-analyzed
   ::ms/description es/string-analyzed
   ::ms/provenance {:properties {::ms/source es/string-stored-not_analyzed
                                 :kixi.user/id es/string-stored-not_analyzed
                                 ::ms/parent-id es/string-stored-not_analyzed
                                 ::ms/created es/timestamp}}
   ::ms/sharing {:properties (zipmap ms/activities
                                     (repeat es/string-stored-not_analyzed))}})

(def local-es
  {:host "localhost"
   :port 9200
   :native-port 9300})

(defn create-index
  [db]
  (esi/create (client db)
              index-name
              {:mappings {doc-type
                          {:properties (es/all-keys->es-format doc-def)}}
               :settings {}}))

(def merge-data
  (partial es/merge-data index-name doc-type))

(defn local-native-conn
  []
  (es/connect [[(:host local-es)
                (:native-port local-es)]]
              nil))

(defn insert-metadata
  [db metadata]
  (merge-data
   db
   (::ms/id metadata)
   metadata))
