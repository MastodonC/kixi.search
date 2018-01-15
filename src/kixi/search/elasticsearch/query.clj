(ns kixi.search.elasticsearch.query
  (:require [com.stuartsierra.component :as component]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [kixi.search.elasticsearch :as es]
            [kixi.comms :as comms]
            [taoensso.timbre :as timbre :refer [info]]))

(defprotocol Query
  (find-by-id- [this id])
  (find-by-query- [this query-map from-index cnt sort-by sort-order]))

(s/def ::query
  (let [ex #(ex-info "Use stubbed fn version." {:fn %})]
    (s/with-gen
      (partial satisfies? Query)
      #(gen/return (reify Query
                     (find-by-id- [this id] (throw (ex "id")))
                     (find-by-query- [this query-map from-index cnt sort-by sort-order] (throw (ex "query"))))))))

(s/fdef find-by-id
        :args (s/cat :impl ::query
                     :id string?)
        )

(defn find-by-id
  [impl id]
  (find-by-id- impl id))

(defn find-by-query
  [impl query-map from-index cnt sort-by sort-order]
  (find-by-query- impl query-map from-index cnt sort-by sort-order))

(def sfirst (comp second first))

(def index-name "kixi-datastore_file-metadata")
(def doc-type "file-metadata")

(defrecord ElasticSearch
    [communications host port native-port cluster discover migrators-dir es-url]

  Query
  (find-by-id-
    [this id]
    (es/get-document index-name doc-type es-url id))

  (find-by-query-
    [this query-map from-index cnt sort-by sort-order]
    (es/search-data index-name
                    doc-type
                    es-url
                    query-map
                    from-index
                    cnt
                    sort-by
                    sort-order))

  component/Lifecycle
  (start [component]
    (if-not es-url
      (let [{:keys [native-host-ports http-host-ports]} {:http-host-ports [[host port]]}
            joplin-conf {:migrators {:migrator "joplin/kixi/datastore/metadatastore/migrators/"}
                         :databases {:es {:type :es :host (ffirst http-host-ports)
                                          :port (sfirst http-host-ports)
                                          :migration-index "metadatastore-migrations"}}
                         :environments {:env [{:db :es :migrator :migrator}]}}]
        (info "Starting File Metadata ElasticSearch Store")
        ;;joplin ES is using elastisch 2.x via native, not going to work
        ;;(es/migrate :env joplin-conf)
        (assoc component :es-url
               (str "http://" host ":" port)))
      component))
  (stop [component]
    (if es-url
      (do (info "Destroying File Metadata ElasticSearch Store")
          (dissoc component :es-url))
      component)))
