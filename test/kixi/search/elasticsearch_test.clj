(ns kixi.search.elasticsearch-test
  (:require [kixi.search.elasticsearch :as sut]
            [clojure.test :as t :refer :all]))

(deftest select-nested-test
  (testing "returns nil when no matches"
    (is (= nil
           (sut/select-nested {:a {:ab 1 :abb 2} :b {:bb 2} :c {:ab 1}}
                              :ax))))
  (testing "nested selection"
    (is (= {:a 1 :c 1}
           (sut/select-nested {:a {:ab 1 :abb 2} :b {:bb 2} :c {:ab 1}}
                              :ab)))))

(deftest submaps-with-test
  (is (= {:a {:a 1 :b 2} :c {:a 1}}
         (sut/submaps-with {:a {:a 1 :b 2} :b {:b 2} :c {:a 1}}
                           :a))))

(deftest query->es-filter-test
  (testing "Filter only query"
    (is (= {:query
            {:bool
             {:filter
              {:terms {"sharing.meta-read" ["123"]}}}}}
           (sut/query->es-filter {:query
                                  {:sharing
                                   {:meta-read {:contains ["123"]}}}}))))
  (testing "Filter and matchers"
    (is (= {:query
            {:bool
             {:must {:match {"name" "x"}}
              :filter
              {:terms {"sharing.meta-read" ["123"]}}}}}
           (sut/query->es-filter {:query
                                  {:name {:match "x"}
                                   :sharing
                                   {:meta-read {:contains ["123"]}}}})))))
