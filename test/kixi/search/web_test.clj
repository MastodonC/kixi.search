(ns kixi.search.web-test
  (:require [kixi.search.web :as sut]
            [clojure.test :as t]
            [kixi.spec :refer [alias]]
            [medley.core :refer [map-vals]]
            [com.rpl.specter :as specter :refer [MAP-VALS]]
            [clojure.spec.alpha :as spec]))

(alias 'ms 'kixi.datastore.metadatastore)
(alias 'msq 'kixi.datastore.metadatastore.query)

(defn set-params
  "Reordering occurs during process, set the query paramters so equality words"
  [m]
  (specter/transform
   [MAP-VALS MAP-VALS]
   set
   m))

(t/deftest ensure-group-access-test
  (t/testing "Submitted no filter groups, all user groups in meta-read"
    (t/is (= {::msq/meta-read {:contains #{1 2 3}}}
             (set-params (sut/ensure-group-access [1 2 3] nil)))))
  (t/testing "Submitted meta-read groups, only those remain"
    (t/is (= {::msq/meta-read {:contains #{1}}}
             (set-params (sut/ensure-group-access
                          [1 2 3]
                          {::msq/meta-read {:contains [1]}})))))
  (t/testing "Submitted meta-read groups, only valid remain"
    (t/is (= {::msq/meta-read {:contains #{1}}}
             (set-params (sut/ensure-group-access
                          [1 2 3]
                          {::msq/meta-read {:contains [1 4]}})))))
  (t/testing "Submitted file-read, only valid-remain"
    (t/is (= {::msq/meta-read {:contains #{1 2 3}}
              ::msq/file-read {:contains #{1}}}
             (set-params (sut/ensure-group-access
                          [1 2 3]
                          {::msq/file-read {:contains [1 4]}}))))))