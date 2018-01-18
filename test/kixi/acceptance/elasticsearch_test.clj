(ns kixi.acceptance.elasticsearch-test
  (:require [clojure.test :refer :all]
            [clj-time.core :as t]
            [environ.core :as env]
            [kixi.search.elasticsearch :as sut]
            [kixi.datastore.metadatastore :as md]
            [kixi.search.elasticsearch.event-handlers.metadata-create :as mc]
            [kixi.spec.conformers :as conformers]
            [kixi.spec :refer [alias]]))

(alias 'kixi.datastore.metadatastore.query 'mq)
(alias 'kixi.user 'user)

(defn elasticsearch-url
  [{:keys [host port]}]
  (str "http://" host ":" port))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(def es-url
  (elasticsearch-url {:host (env/env :elasticsearch-host "localhost")
                      :port (env/env :elasticsearch-port "9200")}))

(def insert-data (partial sut/insert-data mc/index-name mc/doc-def es-url))
(def search-data (partial sut/insert-data mc/index-name mc/doc-def es-url))

(defn ensure-index
  [all-tests]
  (when-not (sut/index-exists? es-url mc/index-name)
    (sut/create-index es-url mc/index-name mc/doc-type mc/doc-def))
  (all-tests))

(defn file-event
  [user-id & [overrides]]
  (merge-with merge
              {::md/type "stored"
               ::md/file-type "csv"
               ::md/id user-id
               ::md/name "Test File"
               ::md/provenance {::md/source "upload"
                                ::user/id user-id
                                ::md/created (conformers/time-unparser (t/now))}
               ::md/size-bytes 10
               ::md/sharing {::md/file-read [user-id]
                             ::md/meta-update [user-id]
                             ::md/meta-read [user-id]}}
              (or overrides {})))


;;(use-fixtures :once ensure-index)

(comment "not yet"
         (deftest search-by-id
           (let [uid (uuid)
                 data (file-event uid)]
             (insert-data uid file-event)
             (prn (search-data {:query {::mq/id {:match uid}}})))))
