(ns kixi.search.query-model
  (:require [clojure.spec.alpha :as s]
            [kixi.spec :refer [alias]]
            [com.rpl.specter :as sp]))

;; TODO generate these

(alias 'ms 'kixi.datastore.metadatastore)
(alias 'msq 'kixi.datastore.metadatastore.query)

(defn query-spec-name
  [spec]
  (keyword (str (namespace spec) ".query") (name spec)))

(defn query-spec
  [[spec actions]]
  (eval
   `(s/def ~(query-spec-name spec)
      (s/map-of ~actions
                (partial s/valid? ~spec)))))

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
   ::ms/sharing {::ms/meta-read #{:contains}}
   ::ms/provenance {::ms/created #{:gt :gte :lt :lte}}})

(create-query-specs metadata->query-actions)

(defmacro define-query-spec
  []
  `(s/def ::query
     (s/and (s/keys :opt ~(mapv query-spec-name (keys metadata->query-actions)))
            (s/every-kv ~(into #{} (mapv query-spec-name (keys metadata->query-actions)))
                        (constantly true)))))

(define-query-spec)


;;TODO this is not spohisticated enough, should assert
;;correct fields are nested at the right levels
(def field-list
  (letfn [(collect [m]
            (when (map? m)
              (concat (keys m)
                      (flatten (keep collect (vals m))))))]
    (set (collect metadata->query-actions))))

(s/def ::fields
  (s/every (s/or :field field-list
                 :nested-field ::fields)
           :kind vector?))

(def sort-orders
  #{:asc :desc})

(s/def ::sort-by
  (s/every (s/or :sort-by field-list
                 :nested-sort-by (s/map-of field-list
                                           ::sort-by)
                 :sorting (s/and (s/map-of field-list
                                           sort-orders)
                                 #(= 1 (count (keys %)))))
           :kind vector?))

(s/def ::from integer?)
(s/def ::size integer?)

(s/def ::query-map
  (s/or :nil nil?
        :query (s/keys :opt-un [::query
                                ::fields
                                ::sort-by
                                ::from
                                ::size])))
