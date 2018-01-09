(ns kixi.search.web-test
  (:require [kixi.search.web :as sut]
            [clojure.test :as t]
            [kixi.spec :refer [alias]]
            [medley.core :refer [map-vals]]))

(alias 'ms 'kixi.datastore.metadatastore)

(t/deftest ensure-group-access-test
  (t/testing "Submitted no filter groups, all user groups in meta-read"
    (t/is (= {::ms/meta-read #{1 2 3}}
             (map-vals set (sut/ensure-group-access [1 2 3] nil)))))
  (t/testing "Submitted meta-read groups, only those remain"
    (t/is (= {::ms/meta-read #{1}}
             (map-vals set (sut/ensure-group-access [1 2 3] {::ms/meta-read [1]})))))
  (t/testing "Submitted meta-read groups, only valid remain"
    (t/is (= {::ms/meta-read #{1}}
             (map-vals set (sut/ensure-group-access [1 2 3] {::ms/meta-read [1 4]})))))
  (t/testing "Submitted file-read, only valid-remain"
    (t/is (= {::ms/meta-read #{1 2 3}
              ::ms/file-read #{1}}
             (map-vals set (sut/ensure-group-access [1 2 3] {::ms/file-read [1 4]}))))))
