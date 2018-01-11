(ns kixi.search.query-model
  (:require [clojure.spec.alpha :as s]
            [kixi.spec :refer [alias]]))

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
  (s/keys :opt-un [::array/contains]))

(s/def ::msq/sharing
  (s/keys :opt-un [::msq/meta-read]))
(s/def ::msq/tags
  (s/keys ))

(s/def ::query
  (s/and (s/keys :opt [::msq/name
                       ::msq/description
                       ::msq/sharing])
         (s/every-kv #{::msq/name
                       ::msq/description
                       ::msq/sharing}
                     (constantly true))))

;; A small set of selectable fields, use when using auto complete searches
(s/def ::fields
  (s/every #{::ms/name}
           :kind vector?))

(s/def ::query-map
  (s/or :nil nil?
        :query (s/keys :opt-un [::query ::fields])))
