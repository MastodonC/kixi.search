(ns kixi.search.graphql-test
  (:require  [clojure.test :as t :refer [is deftest testing]]
             [clojure.spec.test.alpha :as spec-test]
             [kixi.search.graphql :as sut]
             [clojure.spec.alpha :as spec]
             [kixi.spec.test-helper :refer [is-match]]))

(deftest check-keyword->graphql-keyword
  (testing "simple keyword"
    (is (= :asd
           (sut/keyword->graphql-keyword :asd)))
    (is (= :thing_that
           (sut/keyword->graphql-keyword :thing.that)))
    (is (= :thing__that
           (sut/keyword->graphql-keyword :thing-that))))
  (testing "qualified keyword"
    (is (= :kixi_user___id
           (sut/keyword->graphql-keyword :kixi.user/id)))
    (is (= :kixi_thing___stuff__id
           (sut/keyword->graphql-keyword :kixi.thing/stuff-id)))))

(deftest check-graphql-keyword->keyword
  (testing "simple keyword"
    (is (= :asd
           (sut/graphql-keyword->keyword :asd)))
    (is (= :thing.that
           (sut/graphql-keyword->keyword :thing_that)))
    (is (= :thing-that
           (sut/graphql-keyword->keyword :thing__that))))
  (testing "qualified keyword"
    (is (= :kixi.user/id
           (sut/graphql-keyword->keyword :kixi_user___id)))
    (is (=  :kixi.thing/stuff-id
            (sut/graphql-keyword->keyword :kixi_thing___stuff__id)))))

(def test-schema
  '{:objects
    {:metadata
     {:fields {:kixi.datastore.metadatastore/id {:type (non-null Int)}
               :kixi.datastore.metadatastore/name {:type (non-null String)}
               :kixi.datastore.metadatastore/type {:type (non-null String)}
               :kixi.datastore.metadatastore/description {:type String}
               :kixi.datastore.metadatastore/sharing {:type (non-null :sharing-matrix)}}}

     :sharing-matrix
     {:fields {:meta-read (list String)
               :meta-update (list String)}}

     :user
     {:fields {:kixi.user/id {:type (non-null String)}}}

     :group
     {:fields {:kixi.group/id {:type (non-null String)}}}}

    :queries
    {:metadata {:type (non-null (list :metadata))
                :args {:kixi.datastore.metadatastore/id {:type (non-null Int)}
                       :kixi.datastore.metadatastore/sharing {:meta-read (list (non-null :group))
                                                              :meta-update (list :group)}}
                :resolve :get-metadata}}})

(def test-graphql-schema
  '{:objects
    {:metadata
     {:fields {:kixi_datastore_metadatastore___id {:type (non-null Int)}
               :kixi_datastore_metadatastore___name {:type (non-null String)}
               :kixi_datastore_metadatastore___type {:type (non-null String)}
               :kixi_datastore_metadatastore___description {:type String}
               :kixi_datastore_metadatastore___sharing {:type (non-null :sharing-matrix)}}}

     :sharing__matrix
     {:fields {:meta__read (list String)
               :meta__update (list String)}}

     :user
     {:fields {:kixi_user___id {:type (non-null String)}}}

     :group
     {:fields {:kixi_group___id {:type (non-null String)}}}}

    :queries
    {:metadata {:type (non-null (list :metadata))
                :args {:kixi_datastore_metadatastore___id {:type (non-null Int)}
                       :kixi_datastore_metadatastore___sharing {:meta__read (list (non-null :group))
                                                                :meta__update (list :group)}}
                :resolve :get-metadata}}})

(deftest namespaced-schema->graphql-schema-test
  (is-match test-graphql-schema
            (sut/namespaced-map->graphql-map test-schema)))
