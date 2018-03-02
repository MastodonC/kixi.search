(ns kixi.search.acceptance.elasticsearch-test
  (:require [cheshire.core :as json]
            [clj-time.core :as t]
            [clojure.spec.test.alpha :as spec-test]
            [clojure.test :refer :all]
            [environ.core :refer [env]]
            [kixi.datastore.metadatastore :as md]
            [kixi.search.elasticsearch.client :as sut]
            [kixi.search.metadata.event-handlers.update :as mc]
            [kixi.spec :refer [alias]]
            [kixi.spec.conformers :as conformers]
            [kixi.spec.test-helper :refer [wait-is= is-submap]]
            [kixi.user :as user]
            [taoensso.timbre :as timbre :refer [error]]
            [kixi.search.elasticsearch.index-manager :as index-manager]
            [com.stuartsierra.component :as component]))

(timbre/set-level! :warn)

(def wait-tries (Integer/parseInt (env :wait-tries "100")))
(def wait-per-try (Integer/parseInt (env :wait-per-try "10")))

(defn wait-for-pred
  ([p]
   (wait-for-pred p wait-tries))
  ([p tries]
   (wait-for-pred p tries wait-per-try))
  ([p tries ms]
   (loop [try tries]
     (if (pos? try)
       (let [result (p)]
         (if (not result)
           (do
             (Thread/sleep ms)
             (recur (dec try)))
           result))
       (error "Run out of pred attempts!")))))

(defn elasticsearch-url
  [{:keys [protocol host port]}]
  (str protocol "://" host ":" port))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(def es-config
  {:protocol (env :es-protocol "http")
   :host (env :es-host "localhost")
   :port (env :es-port "9200")
   :profile "local"})

(def es-url
  (elasticsearch-url es-config))

(def profile-index
  (str (:profile es-config) "-kixi-search_metadata"))

(def get-by-id (partial sut/get-by-id profile-index mc/doc-type es-url))
(def get-by-id-raw- (partial sut/get-by-id-raw- profile-index mc/doc-type es-url))
(def search-data (partial sut/search-data profile-index mc/doc-type es-url))
(def apply-func (partial sut/apply-func profile-index mc/doc-type es-url))

(defn wait-for-indexed
  [id]
  (wait-for-pred #((comp first :items) (search-data {:query {::md/id {:equals id}}}))))

(defn insert-data
  [id data]
  (sut/insert-data profile-index
                   mc/doc-type
                   es-url
                   id
                   data))

(defn ensure-index
  [all-tests]
  (let [index-manager (component/start (index-manager/map->IndexManager es-config))]
    (all-tests)
    (component/stop index-manager)))

(defn instrument
  [all-tests]
  (spec-test/instrument)
  (all-tests)
  (spec-test/unstrument))

(defn file-event
  [user-id & [overrides]]
  (merge
   {::md/type "stored"
    ::md/file-type "csv"
    ::md/id user-id
    ::md/name "Test File"
    ::md/provenance {::md/source "upload"
                     ::user/id user-id
                     ::md/created (conformers/time-unparser (t/now))}
    ::md/size-bytes 10
    ::md/sharing {::md/file-read [user-id]
                  ::md/meta-update [user-id]
                  ::md/meta-read [user-id]}}
   (or overrides {})))


(use-fixtures :once
  ensure-index
  instrument)

(deftest test-get-by-id
  (let [uid (uuid)
        data (file-event uid)]
    (insert-data uid data)
    (wait-is= data
              (get-by-id uid uid))
    (wait-is= nil
              (get-by-id uid (uuid)))))

(deftest search-by-id
  (let [uid (uuid)
        data (file-event uid)]
    (insert-data uid data)
    (wait-is= data
              ((comp first :items) (search-data {:query {::md/id {:equals uid}}})))))

(deftest search-by-name-and-sharing
  (let [uid (uuid)
        data (file-event uid)]
    (insert-data uid data)
    (wait-is= data
              ((comp first :items) (search-data {:query {::md/name {:match "Test File"}
                                                         ::md/sharing {::md/meta-read {:contains [uid]}}}})))))

(deftest search-by-sharing-and-type
  (let [uid (uuid)
        uid2 (uuid)
        data-one (file-event uid)
        data-two (file-event uid
                             {::md/type "bundle"
                              ::md/id uid2})]
    (insert-data uid data-one)
    (insert-data uid2 data-two)
    (wait-is= 2
              ((comp count :items) (search-data {:query {::md/sharing {::md/meta-read {:contains [uid]}}}})))
    (wait-is= 1
              ((comp count :items) (search-data {:query {::md/name {:match "Test File"}
                                                         ::md/type {:equals "stored"}
                                                         ::md/sharing {::md/meta-read {:contains [uid]}}}})))
    (wait-is= data-one
              ((comp first :items) (search-data {:query {::md/type {:equals "stored"}
                                                         ::md/sharing {::md/meta-read {:contains [uid]}}}})))))

(deftest search-by-multiple-elements
  (let [uid (uuid)
        data (file-event uid)]
    (insert-data uid data)
    (wait-is= data
              ((comp first :items) (search-data {:query {::md/name {:match "Test File"}
                                                         ::md/type {:equals "stored"}
                                                         ::md/sharing {::md/meta-read {:contains [uid]}}}})))
    (wait-is= data
              ((comp first :items) (search-data {:query {::md/name {:match "Test File"}
                                                         ::md/sharing {::md/meta-read {:contains [uid]}
                                                                       ::md/meta-update {:contains [uid]}}}})))
    (wait-is= data
              ((comp first :items) (search-data {:query {::md/name {:match "Test File"}
                                                         ::md/type {:equals "stored"}
                                                         ::md/sharing {::md/meta-read {:contains [uid]}
                                                                       ::md/meta-update {:contains [uid]}}}})))
    (wait-is= data
              ((comp first :items) (search-data {:query {::md/name {:match "Test File"}
                                                         ::md/type {:equals "stored"}
                                                         ::md/file-type {:equals "csv"}
                                                         ::md/sharing {::md/meta-read {:contains [uid]}
                                                                       ::md/meta-update {:contains [uid]}}}})))
    (wait-is= data
              ((comp first :items) (search-data {:query {::md/name {:match "Test File"}
                                                         ::md/provenance {::md/source {:equals "upload"}}
                                                         ::md/type {:equals "stored"}
                                                         ::md/file-type {:equals "csv"}
                                                         ::md/sharing {::md/meta-read {:contains [uid]}
                                                                       ::md/meta-update {:contains [uid]}}}})))
    (wait-is= nil
              ((comp first :items) (search-data {:query {::md/name {:match "Test File"}
                                                         ::md/provenance {::md/source {:equals "upload"}}
                                                         ::md/type {:equals "stored"
                                                                    :exists true}
                                                         ::md/file-type {:equals "csv"}
                                                         ::md/tombstone {:exists false}
                                                         ::md/size-bytes {:exists false}
                                                         ::md/id {:exists true}
                                                         ::md/sharing {::md/meta-read {:contains [uid]}
                                                                       ::md/meta-update {:contains [uid]}}}})))))

(deftest search-by-id-filter-fields
  (testing "Top level field"
    (let [uid (uuid)
          data (file-event uid)]
      (insert-data uid data)
      (wait-is= (select-keys data [::md/name])
                ((comp first :items) (search-data {:query {::md/id {:equals uid}}
                                                   :fields [::md/name]})))))
  (testing "Nested field"
    (let [uid (uuid)
          data (file-event uid)]
      (insert-data uid data)
      (wait-is= {::md/provenance
                 {::md/created
                  (get-in data
                          [::md/provenance ::md/created])}}
                ((comp first :items) (search-data {:query {::md/id {:equals uid}}
                                                   :fields [[::md/provenance ::md/created]]})))))
  (testing "Both"
    (let [uid (uuid)
          data (file-event uid)]
      (insert-data uid data)
      (wait-is= {::md/name (::md/name data)
                 ::md/provenance
                 {::md/created
                  (get-in data
                          [::md/provenance ::md/created])}}
                ((comp first :items) (search-data {:query {::md/id {:equals uid}}
                                                   :fields [::md/name [::md/provenance ::md/created]]}))))))

(deftest search-by-id-sorting
  (let [first-id (uuid)
        first-data (file-event first-id)
        _ (insert-data first-id first-data)
        second-id (uuid)
        second-data (file-event second-id)
        _ (insert-data second-id second-data)]
    (wait-is= [first-data second-data]
              (:items
               (search-data {:query {::md/id {:contains [first-id second-id]}}
                             :sort-by [{::md/provenance {::md/created :asc}}]})))
    (wait-is= [first-data second-data]
              (:items
               (search-data {:query {::md/id {:contains [first-id second-id]}}
                             :sort-by [{::md/provenance ::md/created}]})))
    (wait-is= [second-data first-data]
              (:items
               (search-data {:query {::md/id {:contains [first-id second-id]}}
                             :sort-by [{::md/provenance {::md/created :desc}}]})))))

(deftest search-by-id-paging
  (let [first-id (uuid)
        first-data (file-event first-id)
        _ (insert-data first-id first-data)
        second-id (uuid)
        second-data (file-event second-id)
        _ (insert-data second-id second-data)
        third-id (uuid)
        third-data (file-event third-id)
        _ (insert-data third-id third-data)]
    (wait-is= {:items [first-data second-data third-data]
               :paging {:total 3
                        :count 3
                        :index 0}}
              (search-data {:query {::md/id {:contains [first-id second-id third-id]}}
                            :sort-by [{::md/provenance {::md/created :asc}}]}))
    (wait-is= {:items [first-data second-data]
               :paging {:total 3
                        :count 2
                        :index 0}}
              (search-data {:query {::md/id {:contains [first-id second-id third-id]}}
                            :sort-by [{::md/provenance {::md/created :asc}}]
                            :size 2}))
    (wait-is= {:items [second-data third-data]
               :paging {:total 3
                        :count 2
                        :index 1}}
              (search-data {:query {::md/id {:contains [first-id second-id third-id]}}
                            :sort-by [{::md/provenance {::md/created :asc}}]
                            :from 1}))))

(deftest apply-update-function
  (testing "Original doesn't exist"
    (is (thrown-with-msg? Exception
                          #"clj-http: status 400"
                          (apply-func (uuid) #(assoc % :new-field 1)))))
  (testing "Simple valid update"
    (let [uid (uuid)
          data (file-event uid)
          _ (insert-data uid data)]
      (wait-is= data
                ((comp first :items) (search-data {:query {::md/id {:equals uid}}})))
      (apply-func uid #(assoc % :new-field 1))
      (wait-is= (assoc data :new-field 1)
                ((comp first :items) (search-data {:query {::md/id {:equals uid}}})))
      (wait-is= 2
                (-> (get-by-id-raw- uid true)
                    :body
                    (json/parse-string keyword)
                    :_version))))
  (testing "Concurrent update failure"
    (let [uid (uuid)
          data (file-event uid)
          _ (insert-data uid data)]
      (wait-is= data
                ((comp first :items) (search-data {:query {::md/id {:equals uid}}})))
      (let [first-resp (future (apply-func uid #(do (Thread/sleep 1000)
                                                    (assoc % :first 1))))
            second-resp (future (apply-func uid #(assoc % :second 2)))]
        (is (thrown-with-msg? Exception
                              #"clj-http: status 409"
                              @first-resp))
        (is (= 200
               (:status @second-resp)))))))

(deftest search-by-id-not-tombstoned
  (let [uid (uuid)
        data (file-event uid)]
    (insert-data uid data)
    (wait-is= data
              ((comp first :items) (search-data {:query {::md/id {:equals uid}
                                                         ::md/tombstone {:exists false}}})))
    (wait-is= nil
              ((comp first :items) (search-data {:query {::md/id {:equals uid}
                                                         ::md/tombstone {:exists true}}})))))

(deftest search-by-id-tombstoned
  (let [uid (uuid)
        data (file-event uid {::md/tombstone true})]
    (insert-data uid data)
    (wait-is= data
              ((comp first :items) (search-data {:query {::md/id {:equals uid}
                                                         ::md/tombstone {:exists true}}})))
    (wait-is= nil
              ((comp first :items) (search-data {:query {::md/id {:equals uid}
                                                         ::md/tombstone {:exists false}}})))))

(deftest search-by-name-and-sharing-not-tombstoned
  (let [uid (uuid)
        data (file-event uid)]
    (insert-data uid data)
    (wait-is= data
              ((comp first :items) (search-data {:query {::md/tombstone {:exists false}
                                                         ::md/name {:match "Test File"}
                                                         ::md/sharing {::md/meta-read {:contains [uid]}}}})))
    (wait-is= nil
              ((comp first :items) (search-data {:query {::md/tombstone {:exists true}
                                                         ::md/name {:match "Test File"}
                                                         ::md/sharing {::md/meta-read {:contains [uid]}}}})))))

(deftest search-by-name-and-sharing-tombstoned
  (let [uid (uuid)
        data (file-event uid {::md/tombstone true})]
    (insert-data uid data)
    (wait-is= data
              ((comp first :items) (search-data {:query {::md/tombstone {:exists true}
                                                         ::md/name {:match "Test File"}
                                                         ::md/sharing {::md/meta-read {:contains [uid]}}}})))
    (wait-is= nil
              ((comp first :items) (search-data {:query {::md/tombstone {:exists false}
                                                         ::md/name {:match "Test File"}
                                                         ::md/sharing {::md/meta-read {:contains [uid]}}}})))
    (wait-is= nil
              (get-by-id uid uid))))
