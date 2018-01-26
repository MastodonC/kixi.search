(ns kixi.acceptance.elasticsearch-test
  (:require [clj-time.core :as t]
            [clojure.spec.test.alpha :as spec-test]
            [clojure.test :refer :all]
            [environ.core :refer [env]]
            [kixi.datastore.metadatastore :as md]
            [kixi.search.elasticsearch :as sut]
            [kixi.search.elasticsearch.event-handlers.metadata-create :as mc]
            [kixi.spec :refer [alias]]
            [kixi.spec.conformers :as conformers]
            [kixi.user :as user]
            [taoensso.timbre :as timbre]))

(alias 'kixi.datastore.metadatastore.query 'mq)

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
       (prn "Run out of pred attempts!")))))

(defn elasticsearch-url
  [{:keys [protocol host port]}]
  (str protocol "://" host ":" port))

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(def es-url
  (elasticsearch-url {:protocol (env :es-protocol "http")
                      :host (env :es-host "localhost")
                      :port (env :es-port "9200")}))
(def es-url "https://vpc-staging-kixi-search-lg6tvmynyrm2ckwwfnntomrxs4.eu-central-1.es.amazonaws.com:443")

(def get-by-id (partial sut/get-by-id mc/index-name mc/doc-type es-url))
(def search-data (partial sut/search-data mc/index-name mc/doc-type es-url))

(defn wait-for-indexed
  [id]
  (wait-for-pred #((comp first :items) (search-data {:query {::md/id {:equals id}}}))))

(defn insert-data
  [id data]
  (sut/insert-data mc/index-name
                   mc/doc-type
                   es-url
                   id
                   data)
  (wait-for-indexed id))

(defn ensure-index
  [all-tests]
  (when-not (sut/index-exists? es-url mc/index-name)
    (sut/create-index es-url mc/index-name mc/doc-type mc/doc-def)    )
  (all-tests))

(defn instrument
  [all-tests]
  (spec-test/instrument)
  (all-tests)
  (spec-test/unstrument))

(defn file-event
  [user-id & [overrides]]
  (merge-with merge
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
    (is (= data
           (get-by-id uid)))))

(deftest search-by-id
  (let [uid (uuid)
        data (file-event uid)]
    (insert-data uid data)
    (is (= data
           ((comp first :items) (search-data {:query {::md/id {:equals uid}}}))))))

(deftest search-by-id-filter-fields
  (testing "Top level field"
    (let [uid (uuid)
          data (file-event uid)]
      (insert-data uid data)
      (is (= (select-keys data [::md/name])
             ((comp first :items) (search-data {:query {::md/id {:equals uid}}
                                                :fields [::md/name]}))))))
  (testing "Nested field"
    (let [uid (uuid)
          data (file-event uid)]
      (insert-data uid data)
      (is (= {::md/provenance
              {::md/created
               (get-in data
                       [::md/provenance ::md/created])}}
             ((comp first :items) (search-data {:query {::md/id {:equals uid}}
                                                :fields [[::md/provenance ::md/created]]}))))))
  (testing "Both"
    (let [uid (uuid)
          data (file-event uid)]
      (insert-data uid data)
      (is (= {::md/name (::md/name data)
              ::md/provenance
              {::md/created
               (get-in data
                       [::md/provenance ::md/created])}}
             ((comp first :items) (search-data {:query {::md/id {:equals uid}}
                                                :fields [::md/name [::md/provenance ::md/created]]})))))))

(deftest search-by-id-sorting
  (let [first-id (uuid)
        first-data (file-event first-id)
        _ (insert-data first-id first-data)
        second-id (uuid)
        second-data (file-event second-id)
        _ (insert-data second-id second-data)]
    (is (= [first-data second-data]
           (:items
            (search-data {:query {::md/id {:contains [first-id second-id]}}
                          :sort-by [{::md/provenance {::md/created :asc}}]}))))
    (is (= [first-data second-data]
           (:items
            (search-data {:query {::md/id {:contains [first-id second-id]}}
                          :sort-by [{::md/provenance ::md/created}]}))))
    (is (= [second-data first-data]
           (:items
            (search-data {:query {::md/id {:contains [first-id second-id]}}
                          :sort-by [{::md/provenance {::md/created :desc}}]}))))))

(deftest serch-by-id-paging
  (let [first-id (uuid)
        first-data (file-event first-id)
        _ (insert-data first-id first-data)
        second-id (uuid)
        second-data (file-event second-id)
        _ (insert-data second-id second-data)
        third-id (uuid)
        third-data (file-event third-id)
        _ (insert-data third-id third-data)]
    (is (= {:items [first-data second-data third-data]
            :paging {:total 3
                     :count 3
                     :index 0}}
           (search-data {:query {::md/id {:contains [first-id second-id third-id]}}
                         :sort-by [{::md/provenance {::md/created :asc}}]})))
    (is (= {:items [first-data second-data]
            :paging {:total 3
                     :count 2
                     :index 0}}
           (search-data {:query {::md/id {:contains [first-id second-id third-id]}}
                         :sort-by [{::md/provenance {::md/created :asc}}]
                         :size 2})))
    (is (= {:items [second-data third-data]
            :paging {:total 3
                     :count 2
                     :index 1}}
           (search-data {:query {::md/id {:contains [first-id second-id third-id]}}
                         :sort-by [{::md/provenance {::md/created :asc}}]
                         :from 1})))))
