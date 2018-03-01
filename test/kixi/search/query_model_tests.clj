(ns kixi.search.query-model-tests
  (:require [kixi.search.query-model :as sut]
            [clojure.test :as t :refer [deftest is]]
            [kixi.spec :refer [alias]]
            [clojure.spec.alpha :as spec]))

(alias 'msq 'kixi.datastore.metadatastore.query)
(alias 'ms 'kixi.datastore.metadatastore)

(deftest name-query
  (is (spec/valid? ::sut/query-map
                   {:query {::msq/name {:match "a"}}}))
  (is (spec/valid? ::sut/query-map
                   {:query {::msq/name {:match "["}}})))

(deftest name-fields-query
  (is (spec/valid? ::sut/query-map
                   {:query {::msq/name {:match "a"}}
                    :fields [::ms/name ::ms/id
                             [::ms/provenance ::ms/created]
                             [::ms/provenance :kixi.user/id]]})))

(deftest to-homogeneous-test
  (is (= {::ms/name {:predicates #{:match}}
          ::ms/sharing {::ms/meta-read {:predicates #{:contains}}}}
         (sut/to-homogeneous
          {::ms/name #{:match}
           ::ms/sharing {::ms/meta-read #{:contains}}})))
  (is (= {::ms/name {:predicates #{:match}}
          ::ms/foo {:predicates #{:match}}
          ::ms/sharing {::ms/meta-read {:predicates #{:contains}}
                        ::ms/bar {:predicates #{:contains}}}}
         (sut/to-homogeneous
          {::ms/name #{:match}
           ::ms/foo {:predicates #{:match}}
           ::ms/sharing {::ms/meta-read #{:contains}
                         ::ms/bar {:predicates #{:contains}}}}))))

(deftest all-specs-with-actions-test
  (is (= '([:kixi.datastore.metadatastore/name {:predicates #{:match}}]
           [:kixi.datastore.metadatastore/meta-read {:predicates #{:contains}}])
         (sut/all-specs-with-actions {::ms/name {:predicates #{:match}}
                                      ::ms/sharing {::ms/meta-read {:predicates #{:contains}}}}))))
