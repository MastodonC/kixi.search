(ns kixi.search.metadata.event-handlers.update
  (:require [com.stuartsierra.component :as component]
            [kixi.comms :as c]
            [kixi.datastore.metadatastore :as md]
            [kixi.search.elasticsearch.client :as es]
            [kixi.spec :refer [alias]]
            [taoensso.timbre :as timbre :refer [info]]))

(alias 'cs 'kixi.datastore.communication-specs)

(def base-index-name "kixi-search_metadata")
(def doc-type "metadata")

(defmulti update-metadata-processor
  (fn [es-url index-name update-event]
    (::cs/file-metadata-update-type update-event)))

(defmethod update-metadata-processor ::cs/file-metadata-created
  [es-url index-name update-event]
  (let [metadata (::md/file-metadata update-event)]
    (info "Create: " metadata)
    (es/insert-data
     index-name
     doc-type
     es-url
     (::md/id metadata)
     metadata)))

(defmethod update-metadata-processor ::cs/file-metadata-structural-validation-checked
  [es-url index-name update-event]
  (info "Update: " update-event)
  )

(defn sharing-updater
  [current update-event]
  (let [group-id (:kixi.group/id update-event)
        update-fn (case (::md/sharing-update update-event)
                    ::md/sharing-conj #(into [] (cons group-id %))
                    ::md/sharing-disj #(into [] (remove #{group-id} %)))]
    (update-in current
               [::md/sharing (::md/activity update-event)]
               update-fn)))

(defmethod update-metadata-processor ::cs/file-metadata-sharing-updated
  [es-url index-name update-event]
  (info "Update Share: " update-event)
  (es/apply-func index-name doc-type es-url
                 (::md/id update-event)
                 #(sharing-updater % update-event)))

(defn dissoc-nonupdates
  [md]
  (reduce
   (fn [acc [k v]]
     (if (and (namespace k) (clojure.string/index-of (namespace k) ".update"))
       (assoc acc k v)
       acc))
   {}
   md))

(defn- update?
  [kw]
  (and (qualified-keyword? kw)
       (clojure.string/index-of (namespace kw) ".update")))

(defn remove-update-ns
  [kw]
  (when (update? kw)
    (keyword
     (subs (namespace kw)
           0
           (clojure.string/index-of (namespace kw) ".update"))
     (name kw))))

(def update-cmds #{:set :conj :disj})

(defn- update-cmd?
  [x]
  (and (map? x)
       (= 1 (count (keys x)))
       (update-cmds (first (keys x)))))

(def first-key (comp first keys))
(def first-val (comp first vals))

(declare apply-updates)

(defn apply-update
  [current kw update-cmd]
  (if (= :rm update-cmd)
    (dissoc current
            (remove-update-ns kw))
    (let [update-fn (cond
                      (update-cmd? update-cmd)
                      (case (first-key update-cmd)
                        :set (constantly (first-val update-cmd))
                        :conj #(conj % (first-val update-cmd))
                        :disj #(remove (set (vals update-cmd)) %))
                      :else
                      #(apply-updates %
                                      update-cmd))]
      (update current
              (remove-update-ns kw)
              update-fn))))

(defn apply-updates
  [current updates]
  (reduce-kv
   (fn [c kw update-cmd]
     (if (update? kw)
       (apply-update c kw update-cmd)
       c))
   current
   updates))

(defn dissoc-nonupdates
  [md]
  (reduce
   (fn [acc [k v]]
     (if (and (qualified-keyword? k)
              (clojure.string/index-of (namespace k) ".update"))
       (assoc acc k v)
       acc))
   {}
   md))

(defmethod update-metadata-processor ::cs/file-metadata-update
  [es-url index-name update-event]
  (info "Update: " update-event)
  (es/apply-func index-name doc-type es-url
                 (::md/id update-event)
                 #(apply-updates %
                                 (dissoc-nonupdates update-event))))

(defn response-event
  [r]
  nil)

(defn connection->es-url
  [connection]
  (str (:protocol connection) "://" (:host connection) ":" (:port connection 9200)))

(defrecord MetadataCreate
    [communications started protocol host port profile
     es-url profile-index-name]
  component/Lifecycle
  (start [component]
    (if-not started
      (let [es-url (str protocol "://" host ":" port)
            profile-index (str profile "-" base-index-name)]
        (c/attach-event-handler! communications
                                 :kixi.search/metadata-update
                                 :kixi.datastore.file-metadata/updated
                                 "1.0.0"
                                 (comp response-event
                                       (partial update-metadata-processor es-url profile-index)
                                       :kixi.comms.event/payload))
        (assoc component
               :started true
               :es-url es-url
               :profile-index-name profile-index))
      component))
  (stop [component]))
