(ns kixi.search.metadata.event-handlers.bundle-file-remove
  (:require [com.stuartsierra.component :as component]
            [kixi.comms :as c]
            [kixi.datastore.metadatastore :as md]
            [kixi.search.elasticsearch.client :as es]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :as timbre :refer [info]]))


(def base-index-name "kixi-search_metadata")
(def doc-type "metadata")

(defmethod c/event-payload
  [:kixi.datastore/files-removed-from-bundle "1.0.0"]
  [_]
  (s/keys :req [::md/id
                ::md/bundled-ids]))

(defn remove-from-bundle
  [index-name es-url remove-files-event]
  (es/apply-func index-name doc-type es-url
                 (::md/id remove-files-event)
                 (fn [meta]
                   (update meta
                           ::md/bundled-ids
                           #(into [] (remove (set (::md/bundled-ids remove-files-event)) %))))))

(defn response-event
  [r]
  nil)

(defrecord MetadataBundleFileRemove
    [communications started protocol host port profile
     es-url profile-index-name]
  component/Lifecycle
  (start [component]
    (if-not started
      (let [es-url (str protocol "://" host ":" port)
            profile-index (str profile "-" base-index-name)]
        (c/attach-validating-event-handler! communications
                                            :kixi.search/metadata-bundle-remove-files
                                            :kixi.datastore/files-removed-from-bundle
                                            "1.0.0"
                                            (comp response-event
                                                  (partial remove-from-bundle profile-index es-url)))
        (assoc component
               :started true
               :es-url es-url
               :profile-index-name profile-index))
      component))
  (stop [component]
    (if (:started component)
      (assoc component :started false)
      component)))
