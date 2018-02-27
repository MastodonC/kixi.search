(ns kixi.search.query-model-tests
  (:require [kixi.search.query-model :as sut]
            [clojure.test :as t :refer [deftest is]]
            [kixi.spec :refer [alias]]
            [clojure.spec.alpha :as spec]))

(alias 'msq 'kixi.datastore.metadatastore.query)
(alias 'ms 'kixi.datastore.metadatastore)

(deftest name-query
  (is (spec/valid? ::sut/query-map
                   {:query {::msq/name {:match "a"}}})))

(deftest name-fields-query
  (is (spec/valid? ::sut/query-map
                   {:query {::msq/name {:match "a"}}
                    :fields [::ms/name ::ms/id
                             [::ms/provenance ::ms/created]
                             [::ms/provenance :kixi.user/id]]})))
