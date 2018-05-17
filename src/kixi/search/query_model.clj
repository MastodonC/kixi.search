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
  [[spec {:keys [predicates type-override]}]]
  (let [t# (or type-override
               (partial s/valid? spec))]
    (eval
     `(s/def ~(query-spec-name spec)
        (s/map-of ~predicates
                  ~t#)))))

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
    [sp/ALL (sp/if-path [sp/LAST map? (sp/pred #(not (:predicates %)))]
                        [sp/LAST sp/ALL]
                        identity)]
    definition-map)))

(defn sub-maps-with-keys
  [definition-map]
  (distinct
   (mapv (fn [[k v]]
           [k (keys v)])
         (sp/select
          [sp/ALL (sp/selected? sp/LAST map? (sp/pred #(not (:predicates %))))]
          definition-map))))

(defn to-homogeneous
  [definition-map]
  (sp/transform
   [sp/ALL sp/LAST (sp/if-path set?
                               identity
                               [(sp/pred #(not (:predicates %))) sp/ALL sp/LAST set?])]
   #(hash-map :predicates %)
   definition-map))

(defn create-query-specs
  [hetrogeneous-definition-map]
  (let [definition-map (to-homogeneous hetrogeneous-definition-map)
        query-specs (map query-spec (all-specs-with-actions definition-map))
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
  (map-of #{:match} :kixi.datastore.metadatastore/name))

There is the ability to provide a type override, for use when you don't want the query spec to be limited
to the underlying standard spec. Used here to allow users to search for file names, with strings that
are themselves not valid file names.")
(def metadata->query-actions
  {::ms/name {:predicates #{:match}
              :type-override string?}
   ::ms/type #{:equals}
   ::ms/sharing {::ms/meta-read #{:contains}
                 ::ms/meta-update #{:contains}
                 ::ms/bundle-add #{:contains}}
   ::ms/tags #{:contains}
   ::ms/tombstone #{:exists}
   ::ms/provenance {::ms/created #{:gt :gte :lt :lte}}})

(create-query-specs metadata->query-actions)

(defmacro define-query-spec
  []
  `(s/def ::query
     (s/and (s/keys :opt ~(mapv query-spec-name (keys metadata->query-actions)))
            (s/every-kv ~(into #{} (mapv query-spec-name (keys metadata->query-actions)))
                        (constantly true)))))

(define-query-spec)

(s/def ::fields
  (s/every (s/or :field qualified-keyword?
                 :nested-field ::fields)
           :kind vector?))

(def sort-orders
  #{:asc :desc})

(s/def ::sort-term
  (s/or :sort-by qualified-keyword?
        :nested-sort-by (s/map-of qualified-keyword?
                                  ::sort-term)
        :sorting (s/and (s/map-of qualified-keyword?
                                  sort-orders)
                        #(= 1 (count (keys %))))))

(s/def ::sort-by
  (s/every ::sort-term
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
