(ns kixi.search.query-model
  (:require [clojure.spec.alpha :as s]
            [kixi.spec :refer [alias]]
            [com.rpl.specter :as sp]))

;; TODO generate these

(alias 'ms 'kixi.datastore.metadatastore)
(alias 'msq 'kixi.datastore.metadatastore.query)

(s/def ::contains vector?)
(s/def ::match string?)

(def queryable-string string?)

(s/def ::msq/name
  (s/keys :opt-un [::match]))
(s/def ::msq/description string?)

(s/def ::msq/meta-read
  (s/keys :opt-un [::contains]))

(s/def ::msq/sharing
  (s/keys :opt-un [::msq/meta-read]))
(s/def ::msq/tags
  (s/keys ))

(defn query-spec-name
  [spec]
  (keyword (str (namespace spec) ".query") (name spec)))

(defn query-spec
  [[spec actions]]
  (eval
   `(s/def ~(query-spec-name spec)
      (s/map-of ~actions
                ~spec))))

(defn query-map-spec
  [[map-spec fields]]
  (eval
   `(s/def ~(query-spec-name map-spec)
      (s/and (s/keys
              :opt ~(mapv query-spec-name fields))
             (s/every-kv ~(into #{} (mapv query-spec-name fields))
                         (constantly true))))))

(defn all-specs-with-actions
  [definition-map]
  (distinct
   (sp/select
    [sp/ALL (sp/if-path [sp/LAST map?]
                        [sp/LAST sp/ALL]
                        identity)]
    definition-map)))

(defn sub-maps-with-keys
  [definition-map]
  (distinct
   (mapv (fn [[k v]]
           [k (keys v)])
         (sp/select
          [sp/ALL (sp/selected? sp/LAST map?)]
          definition-map))))

(defn create-query-specs
  [definition-map]
  (let [query-specs (map query-spec (all-specs-with-actions definition-map))
        map-specs (map query-map-spec (sub-maps-with-keys definition-map))]
    (doall (concat query-specs map-specs))))

(comment "Declarative structure for defining valid query commands for metadata fields.
Keys are transformed using 'query-spec-name' which appends 'query' to the namespace,
sets of commands become map-of specs where keys must be in the set and values must adhere to
the orignal unmodified spec.

Example:
(def metadata->query-actions
  {::ms/name #{:match}}

Creates:
(s/def ::msq/name
  (map-of #{:match} :kixi.datastore.metadatastore/name))")
(def metadata->query-actions
  {::ms/name #{:match}
   ::ms/sharing {::ms/meta-read #{:contains}}})

(create-query-specs metadata->query-actions)

(defmacro define-query-spec
  []
  `(s/def ::query
     (s/and (s/keys :opt ~(mapv query-spec-name (keys metadata->query-actions)))
            (s/every-kv ~(into #{} (mapv query-spec-name (keys metadata->query-actions)))
                        (constantly true)))))

(define-query-spec)

;; A small set of selectable fields, use when using auto complete searches
(s/def ::fields
  (s/every #{::ms/name ::ms/id}
           :kind vector?))

(s/def ::query-map
  (s/or :nil nil?
        :query (s/keys :opt-un [::query ::fields])))
