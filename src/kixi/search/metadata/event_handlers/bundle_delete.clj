(ns kixi.search.metadata.event-handlers.bundle-delete
  (:require [com.stuartsierra.component :as component]
            [kixi.comms :as c]
            [kixi.datastore.metadatastore :as md]
            [kixi.search.elasticsearch.client :as es]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :as timbre :refer [info]]))

(def base-index-name "kixi-search_metadata")
(def doc-type "metadata")

(defn response-event
  [r]
  nil)

(defn delete-bundle
  [index-name es-url delete-event]
  (es/apply-func index-name doc-type es-url
                 (::md/id delete-event)
                 #(assoc % ::md/tombstone true ::md/tombstoned-at (:kixi.event/created-at delete-event))))

(defmethod c/event-payload
  [:kixi.datastore/bundle-deleted "1.0.0"]
  [_]
  (s/keys :req [::md/id]))

(defrecord MetadataBundleDelete
    [communications started protocol host port profile
     es-url profile-index-name]
  component/Lifecycle
  (start [component]
    (if-not started
      (let [es-url (str protocol "://" host ":" port)
            profile-index (str profile "-" base-index-name)]
        (c/attach-validating-event-handler! communications
                                            :kixi.search/metadata-bundle-deleted
                                            :kixi.datastore/bundle-deleted
                                            "1.0.0"
                                            (comp response-event
                                                  (partial delete-bundle profile-index es-url)))
        (assoc component
               :started true
               :es-url es-url
               :profile-index-name profile-index))
      component))
  (stop [component]))
