(ns kixi.search.query-model
  (:require [clojure.spec.alpha :as s]
            [kixi.spec :refer [alias]]))

;; TODO generate these

(alias 'ms 'kixi.datastore.metadatastore)
(alias 'msq 'kixi.datastore.metadatastore.query)
(alias 'array 'kixi.search.query-model.list)
(alias 'string 'kixi.search.query-model.string)

(s/def ::array/contains list?)
(s/def ::string/contains string?)

(def queryable-string string?)

(s/def ::msq/name string?)
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

(s/def ::query-map
  (s/or :nil nil?
        :query (s/keys :opt-un [::query])))
