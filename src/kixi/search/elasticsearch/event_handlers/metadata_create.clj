(ns kixi.search.elasticsearch.event-handlers.metadata-create
  (:require [com.stuartsierra.component :as component]
            [kixi.comms :as c]
            [kixi.search.elasticsearch :as es]
            [kixi.spec :refer [alias]]
            [taoensso.timbre :as timbre :refer [info]]
            [kixi.datastore.metadatastore :as md]))

(alias 'cs 'kixi.datastore.communication-specs)

(def index-name "kixi-datastore_file-metadata")
(def doc-type "file-metadata")

(def local-es-url "http://localhost:9200")

(defn insert-metadata
  [es-url metadata]
  (es/insert-data
   index-name
   doc-type
   es-url
   (::md/id metadata)
   metadata))


(defmulti update-metadata-processor
  (fn [conn update-event]
    (::cs/file-metadata-update-type update-event)))


(defmethod update-metadata-processor ::cs/file-metadata-created
  [es-url update-event]
  (let [metadata (::md/file-metadata update-event)]
    (info "Create: " metadata)
    (es/insert-data
     index-name
     doc-type
     es-url
     (::md/id metadata)
     metadata)))

(defmethod update-metadata-processor ::cs/file-metadata-structural-validation-checked
  [conn update-event]
  (info "Update: " update-event)
  )

(defmethod update-metadata-processor ::cs/file-metadata-sharing-updated
  [conn update-event]
  (info "Update Share: " update-event)
  )
(defn dissoc-nonupdates
  [md]
  (reduce
   (fn [acc [k v]]
     (if (and (namespace k) (clojure.string/index-of (namespace k) ".update"))
       (assoc acc k v)
       acc))
   {}
   md))

(defmethod update-metadata-processor ::cs/file-metadata-update
  [conn update-event]
  (info "Update: " update-event)
  )

(defn response-event
  [r]
  nil)

(defn connection->es-url
  [connection]
  (str (:protocol connection) "://" (:host connection) ":" (:port connection 9200)) )

(defrecord MetadataCreate
    [communications protocol host port es-url started]
  component/Lifecycle
  (start [component]
    (if-not started
      (let [es-url   (str protocol "://" host ":" port)]
        (c/attach-event-handler! communications
                                 :kixi.search/metadata-update
                                 :kixi.datastore.file-metadata/updated
                                 "1.0.0"
                                 (comp response-event
                                       (partial update-metadata-processor es-url)
                                       :kixi.comms.event/payload))
        (assoc component
               :started true
               :es-url es-url))
      component))
  (stop [component]))
