(ns kixi.search.elasticsearch-test
  (:require [clojure.test :as t :refer :all]
            [kixi.search.elasticsearch.client :as sut]))

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
             {:must {:match {"name" {:query "x"
                                     :analyzer "standard"}}}
              :should {:span_first {:match {:span_term {"name" "x"}}
                                    :end 1}}
              :filter
              {:terms {"sharing.meta-read" ["123"]}}}}}
           (sut/query->es-filter {:query
                                  {:name {:match "x"}
                                   :sharing
                                   {:meta-read {:contains ["123"]}}}})))))

(deftest empty-string-matchers-removed
  (is (= {:query
          {:bool
           {:filter
            {:terms {"sharing.meta-read" ["123"]}}}}}
         (sut/query->es-filter {:query
                                {:name {:match ""}
                                 :sharing
                                 {:meta-read {:contains ["123"]}}}})
         (sut/query->es-filter {:query
                                {:sharing
                                 {:meta-read {:contains ["123"]}}}}))))

(deftest query->es-filter-exists-test
  (testing "Filter and exists"
    (is (= {:query
            {:bool
             {:must
              {:exists
               {:field "x"}}}}}
           (sut/query->es-filter {:query
                                  {"x" {:exists true}}})))
    (is (= {:query
            {:bool
             {:must_not
              {:exists
               {:field "x"}}}}}
           (sut/query->es-filter {:query
                                  {"x" {:exists false}}})))))

(deftest sorting-constructs
  (is (= [{"provenance.created" "desc"}
          {"name" "asc"}
          {"provenance.created" "asc"}]
         (mapv sut/sort-by->collapsed-es-sorts
               [{:provenance {:created :desc}}
                :name
                {:provenance :created}]))))
