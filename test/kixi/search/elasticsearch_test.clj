(ns kixi.search.elasticsearch-test
  (:require [kixi.search.elasticsearch :as sut]
            [clojure.test :as t :refer :all]))

(deftest select-nested-test
  (is (= {:a 1 :c 1}
         (sut/select-nested {:a {:ab 1} :b {:bb 2} :c {:ab 1}}
                            :ab))))

(deftest query->es-filter-test
  (is (= {:query
          {:bool
           {:filter
            {:terms {"sharing.meta-read" ["123"]}}}}}
         (sut/query->es-filter {:query
                                {:sharing
                                 {:meta-read {:contains ["123"]}}}}))))
