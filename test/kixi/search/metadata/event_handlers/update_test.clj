(ns kixi.search.metadata.event-handlers.update-test
  (:require [kixi.datastore.metadatastore :as md]
            [kixi.search.metadata.event-handlers.update :as sut]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest is testing]]))

(deftest remove-update-ns-test
  (is (= :foo/bar
         (sut/remove-update-ns :foo.update/bar))))

(deftest top-level-set
  (s/def :foo.update/value (s/map-of #{:set} string?))
  (is (= {:foo/value "string"}
         (sut/apply-updates
          {:foo/value 1}
          {:foo.update/value {:set "string"}})))
  (s/def :foo.update/value (s/map-of #{:set} (s/coll-of string?)))
  (is (= {:foo/value ["string"]}
         (sut/apply-updates
          {:foo/value 1}
          {:foo.update/value {:set ["string"]}}))))

(deftest top-level-rm
  (s/def :foo.update/value #{:rm})
  (is (= {}
         (sut/apply-updates
          {:foo/value 1}
          {:foo.update/value :rm}))))

(deftest top-level-conj
  (s/def :foo.update/value (s/map-of #{:conj} int?))
  (is (= {:foo/value [1 2]}
         (sut/apply-updates
          {:foo/value [1]}
          {:foo.update/value {:conj 2}})))
  (is (= {:foo/value [1 2]}
         (sut/apply-updates
          {:foo/value 1}
          {:foo.update/value {:conj 2}})))
  (is (= {:foo/value [2]}
         (sut/apply-updates
          {}
          {:foo.update/value {:conj 2}})))
  (s/def :foo.update/value (s/map-of #{:conj} (s/coll-of int?)))
  (is (= {:foo/value [1 2]}
         (sut/apply-updates
          {:foo/value 1}
          {:foo.update/value {:conj [2]}})))
  (is (= {:foo/value [1 2]}
         (sut/apply-updates
          {:foo/value [1]}
          {:foo.update/value {:conj [2]}})))
  (is (= {:foo/value [2]}
         (sut/apply-updates
          {}
          {:foo.update/value {:conj [2]}}))))

(deftest top-level-disj
  (s/def :foo.update/value (s/map-of #{:disj} int?))
  (is (= {:foo/value []}
         (sut/apply-updates
          {}
          {:foo.update/value {:disj 2}})))
  (is (= {:foo/value [1]}
         (sut/apply-updates
          {:foo/value [1 2]}
          {:foo.update/value {:disj 2}})))
  (is (= {:foo/value [1]}
         (sut/apply-updates
          {:foo/value 1}
          {:foo.update/value {:disj 2}})))
  (is (= {:foo/value []}
         (sut/apply-updates
          {:foo/value 2}
          {:foo.update/value {:disj 2}})))
  (s/def :foo.update/value (s/map-of #{:disj} (s/coll-of int?)))
  (is (= {:foo/value []}
         (sut/apply-updates
          {}
          {:foo.update/value {:disj [2]}})))
  (is (= {:foo/value [1]}
         (sut/apply-updates
          {:foo/value [1 2]}
          {:foo.update/value {:disj [2]}})))
  (is (= {:foo/value [1]}
         (sut/apply-updates
          {:foo/value 1}
          {:foo.update/value {:disj [2]}})))
  (is (= {:foo/value []}
         (sut/apply-updates
          {:foo/value 2}
          {:foo.update/value {:disj [2]}}))))

;; DOOOM DOOOOOOOOOOOOOOOOOOOOOOOOOMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM!!!!
;;
;; The DSL choice of using maps instead of lists means we have no guarentee of ordering
;; we can't trust conj and disj....

(deftest top-level-conj-disj
  (s/def :foo.update/value (s/map-of #{:conj :disj} (s/coll-of int?)))
  (is (= {:foo/value [3]}
         (sut/apply-updates
          {}
          {:foo.update/value {:disj [2]
                              :conj [3]}})))
  ;; in the unlikely event you conj and disj the same value:
  ;; - conj will always be eval'd first
  ;; - disj will always be eval'd second
  (is (= {:foo/value []}
         (sut/apply-updates
          {}
          {:foo.update/value {:conj [3] 
                              :disj [3]}})))
  (is (= {:foo/value []}
         (sut/apply-updates
          {}
          {:foo.update/value {:disj [3] ;; different order should make no difference
                              :conj [3]}}))))

(deftest nested-set
  (is (= {:foo/value {:bar/value "string"}}
         (sut/apply-updates
          {:foo/value {:bar/value 1}}
          {:foo.update/value {:bar.update/value {:set "string"}}}))))

(deftest nested-rm
  (is (= {:foo/value {}}
         (sut/apply-updates
          {:foo/value {:bar/value 1}}
          {:foo.update/value {:bar.update/value :rm}}))))

(deftest multi-nested-update
  (is (= {:foo/value {:bar/value "string"
                      :bar/other "string"
                      :bar/new "string"}}
         (sut/apply-updates
          {:foo/value {:bar/value 1
                       :bar/other 1
                       :bar/also 1}}
          {:foo.update/value {:bar.update/value {:set "string"}
                              :bar.update/other {:set "string"}
                              :bar.update/also :rm
                              :bar.update/new {:set "string"}}}))))

(deftest sharing-updater
  (testing "Add novel"
    (is (= {::md/sharing {:foo ["1"]}}
           (sut/sharing-updater {}
                                {::md/activity :foo
                                 ::md/sharing-update ::md/sharing-conj
                                 :kixi.group/id "1"}))))
  (testing "Add addition"
    (is (= {::md/sharing {:foo ["1" "0"]}}
           (sut/sharing-updater {::md/sharing {:foo ["0"]}}
                                {::md/activity :foo
                                 ::md/sharing-update ::md/sharing-conj
                                 :kixi.group/id "1"}))))
  (testing "Remove empty"
    (is (= {::md/sharing {:foo []}}
           (sut/sharing-updater {}
                                {::md/activity :foo
                                 ::md/sharing-update ::md/sharing-disj
                                 :kixi.group/id "1"}))))
  (testing "Remove present"
    (is (= {::md/sharing {:foo ["0"]}}
           (sut/sharing-updater {::md/sharing {:foo ["0" "1"]}}
                                {::md/activity :foo
                                 ::md/sharing-update ::md/sharing-disj
                                 :kixi.group/id "1"})))))
