(ns kixi.search.metadata.event-handlers.update
  (:require [com.stuartsierra.component :as component]
            [clojure.spec.alpha :as s]
            [kixi.comms :as c]
            [kixi.datastore.metadatastore :as md]
            [kixi.datastore.metadatastore.update :as mdu]
            [kixi.search.elasticsearch.client :as es]
            [kixi.spec :refer [alias] :as ks]
            [taoensso.timbre :as timbre :refer [info]]))

(alias 'cs 'kixi.datastore.communication-specs)

(def base-index-name "kixi-search_metadata")
(def doc-type "metadata")

(defmethod c/event-payload
  [:kixi.datastore/sharing-changed "1.0.0"]
  [_]
  (s/keys :req [::md/id
                ::md/sharing-update
                :kixi.group/id
                ::md/activity]))

(defmulti update-metadata-processor
  (fn [es-url index-name update-event]
    (::cs/file-metadata-update-type update-event)))

(defmethod update-metadata-processor ::cs/file-metadata-created
  [es-url index-name update-event]
  (let [metadata (::md/file-metadata update-event)]
    (info "Create: " metadata)
    (when-not (= "891cd066-ddeb-43f7-bbe3-d854c663c4ad"
                 (::md/id metadata))
      (es/insert-data
       index-name
       doc-type
       es-url
       (::md/id metadata)
       metadata))))

(defmethod update-metadata-processor ::cs/file-metadata-structural-validation-checked
  [es-url index-name update-event]
  (info "Update: " update-event))


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
  (when-not (= "891cd066-ddeb-43f7-bbe3-d854c663c4ad"
               (::md/id update-event)))
  (es/apply-func index-name doc-type es-url
                 (::md/id update-event)
                 #(sharing-updater % update-event)))

(defn sharing-change-processor
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


;; For a description of the metadata fields DSL's look here
;; - https://witanblog.wordpress.com/2018/04/11/the-metadata-update-dsl-for-witan/

(declare apply-updates)

(defn- to-seq
  [x]
  (cond
    (coll? x) x
    (nil? x) x
    :else (vector x)))

(defn apply-update
  [current kw update-dsl]
  (if (= :rm update-dsl)
    (dissoc current
            (remove-update-ns kw))
    ;; update-fn is fn that
    ;; - validates the update DSL for the given kw.update (via spec)
    ;;   else throw an exception
    ;; - if it is found the DSL command is not one of #{:set :conj :disj} then assume
    ;;   it's a nested update and recurse on that key
    (let [update-fn (cond
                      (s/valid? kw update-dsl)
                      (fn [existing]
                        (reduce-kv
                         (fn [a cmd arg]
                           (case cmd
                             :set arg
                             :conj (distinct (concat (to-seq a) (to-seq arg)))
                             :disj (remove (set (to-seq arg)) (to-seq a))
                             (apply-update a cmd arg)))
                         existing
                         ;; ensure the dsl is consistently applied by sorting the map 
                         (apply sorted-map (reduce concat [] update-dsl))))
                      :else
                      (throw (ex-info "Invalid update DSL" (s/explain-data kw update-dsl))))]
      (update current
              (remove-update-ns kw)
              update-fn))))

(defn apply-updates
  "Takes the current backend representation of the metadata as the
  initial value of a reduce applying updates over the metadata
  fields.  The updates are of the form
  
    {ns1.update/kw1 update-dsl1}
     ns2.update/kw2 update-dsl2
     ...}
  "
  [current updates]
  (reduce-kv
   (fn [c kw update-dsl]
     (if (update? kw)
       (apply-update c kw update-dsl)
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
        (c/attach-validating-event-handler! communications
                                            :kixi.search/sharing-update
                                            :kixi.datastore/sharing-changed
                                            "1.0.0"
                                            (comp response-event
                                                  (partial sharing-change-processor es-url profile-index)))
        (assoc component
               :started true
               :es-url es-url
               :profile-index-name profile-index))
      component))
  (stop [component]))
