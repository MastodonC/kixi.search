(ns kixi.search.elasticsearch.query
  (:require [com.stuartsierra.component :as component]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [kixi.search.elasticsearch :as es]
            [kixi.comms :as comms]
            [taoensso.timbre :as timbre :refer [info]]))

(defprotocol Query
  (find-by-id- [this id]))

(s/def ::query
  (let [ex #(ex-info "Use stubbed fn version." {:fn %})]
    (s/with-gen
      (partial satisfies? Query)
      #(gen/return (reify Query
                     (find-by-id- [this id] (throw (ex "id"))))))))

(s/fdef find-by-id
        :args (s/cat :impl ::query
                     :id string?)
        )

(defn find-by-id
  [impl id]
  (find-by-id- impl id))

(def sfirst (comp second first))

(def index-name "kixi-datastore_file-metadata")
(def doc-type "file-metadata")

(defrecord ElasticSearch
    [communications host port native-port cluster discover migrators-dir conn]

  Query
  (find-by-id-
    [this id]
    (es/get-document index-name doc-type conn id))

  component/Lifecycle
  (start [component]
    (if-not conn
      (let [{:keys [native-host-ports http-host-ports]} (if discover
                                                          (es/discover-executor discover)
                                                          {:native-host-ports [[host native-port]]
                                                           :http-host-ports [[host port]]})
            connection (es/connect native-host-ports cluster)
            joplin-conf {:migrators {:migrator "joplin/kixi/datastore/metadatastore/migrators/"}
                         :databases {:es {:type :es :host (ffirst http-host-ports)
                                          :port (sfirst http-host-ports)
                                          :native-port (sfirst native-host-ports)
                                          :migration-index "metadatastore-migrations"}}
                         :environments {:env [{:db :es :migrator :migrator}]}}]
        (info "Starting File Metadata ElasticSearch Store")
        (es/migrate :env joplin-conf)
        (assoc component :conn connection))
      component))
  (stop [component]
    (if conn
      (do (info "Destroying File Metadata ElasticSearch Store")
          (.close (:conn component))
          (dissoc component :conn))
      component)))
