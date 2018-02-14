(ns ^:integration
    kixi.search.integration-tests
  (:require [amazonica.aws.dynamodbv2 :as ddb]
            [clojure.test :refer [deftest is use-fixtures testing]]
            [clj-http.client :as client]
            [clj-time.core :as t]
            [environ.core :refer [env]]
            [user]
            [kixi.datastore.metadatastore :as md]
            [kixi.comms.components.kinesis :as kinesis]
            [kixi.comms :as kcomms]
            [kixi.spec :refer [alias]]
            [kixi.spec.conformers :as conformers]
            [kixi.spec.test-helper :refer [wait-is= is-submap always-is=]]
            [cheshire.core :as json]))

(alias 'cs 'kixi.datastore.communication-specs)
(alias 'mdq 'kixi.datastore.metadatastore.query)
(alias 'mdu 'kixi.datastore.metadatastore.update)
(alias 'event 'kixi.event)

(def search-host (env :search-host "localhost"))
(def search-port (Integer/parseInt (env :search-port "8091")))

(def run-against-staging (Boolean/parseBoolean (env :run-against-staging "false")))

(defn table-exists?
  [endpoint table]
  (try
    (ddb/describe-table {:endpoint endpoint} table)
    (catch Exception e false)))

(defn delete-tables
  [endpoint table-names]
  (doseq [sub-table-names (partition-all 10 table-names)]
    (doseq [table-name sub-table-names]
      (ddb/delete-table {:endpoint endpoint} :table-name table-name))
    (loop [tables sub-table-names]
      (when (not-empty tables)
        (recur (doall (filter (partial table-exists? endpoint) tables)))))))

(defn tear-down-kinesis
  [{:keys [endpoint dynamodb-endpoint streams
           profile app teardown-kinesis teardown-dynamodb]}]
  (when teardown-dynamodb
    (delete-tables dynamodb-endpoint [(kinesis/event-worker-app-name app profile)]))
  (when teardown-kinesis
    (kinesis/delete-streams! endpoint (vals streams))))

(defn cycle-system
  [all-tests & [components]]
  (cond
    (not-empty components)
    (user/start {} components)

    run-against-staging
    (user/start {} [:communications])

    :else
    (user/start))
  (try
    (all-tests)
    (finally
      (let [kinesis-conf (select-keys (:communications @user/system)
                                      [:endpoint :dynamodb-endpoint :streams
                                       :profile :app :teardown-kinesis :teardown-dynamodb])]
        (user/stop)
        (tear-down-kinesis kinesis-conf)))))

(use-fixtures :once cycle-system)

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn namespaced-keyword
  [s]
  (let [splits (clojure.string/split s #"/")]
    (if (second splits)
      (keyword (first splits) (second splits))
      (keyword s))))

(defn get-metadata-by-id
  ([meta-id]
   (get-metadata-by-id meta-id meta-id))
  ([meta-id group-id]
   (some-> (client/get (str "http://" search-host ":" search-port "/metadata/" meta-id)
                       {:headers {:user-groups [group-id]}
                        :throw-exceptions false})
           :body
           (json/parse-string namespaced-keyword))))

(defn search-metadata
  [id n]
  (some-> (client/post (str "http://" search-host ":" search-port "/metadata")
                       {:body (json/generate-string {:query {::mdq/name {:match n}}})
                        :headers {:user-groups [id]}
                        :content-type :json
                        :throw-exceptions false})
          :body
          (json/parse-string namespaced-keyword)))

(defn create-metadata-payload
  [user-id & [overrides]]
  (merge-with merge
              {::cs/file-metadata-update-type ::cs/file-metadata-created
               ::md/file-metadata {::md/type "stored"
                                   ::md/file-type "csv"
                                   ::md/id user-id
                                   ::md/name "Test File"
                                   ::md/provenance {::md/source "upload"
                                                    :kixi.user/id user-id
                                                    ::md/created (conformers/time-unparser (t/now))}
                                   ::md/size-bytes 10
                                   ::md/sharing {::md/file-read [user-id]
                                                 ::md/meta-update [user-id]
                                                 ::md/meta-read [user-id]}}}
              (or overrides {})))

(def first-item (comp first :items))

(defn send-event
  [comms id payload]
  (kcomms/send-event! comms
                      :kixi.datastore.file-metadata/updated
                      "1.0.0"
                      payload
                      {:kixi.comms.event/partition-key id}))

(deftest create-update-search
  (let [comms (:communications @user/system)
        uid (uuid)
        send-event (partial send-event comms uid)
        metadata-payload (create-metadata-payload uid)]

    (testing "Metadata creatable and searchable"
      (send-event metadata-payload)
      (wait-is= (::md/file-metadata metadata-payload)
                (get-metadata-by-id uid))
      (wait-is= (::md/file-metadata metadata-payload)
                (first-item (search-metadata uid "Test File"))))

    (testing "Metadata modifiable"
      (send-event {::cs/file-metadata-update-type ::cs/file-metadata-update
                   ::md/id uid
                   ::mdu/name {:set "Updated Name"}})
      (wait-is= (assoc (::md/file-metadata metadata-payload)
                       ::md/name "Updated Name")
                (first-item (search-metadata uid "Updated Name"))))

    (let [new-group (uuid)
          updated-metadata (-> (::md/file-metadata metadata-payload)
                               (assoc ::md/name "Updated Name")
                               (update-in [::md/sharing ::md/meta-read]
                                          #(into [] (cons new-group %))))]

      (testing "New group access grantable"
        (send-event {::cs/file-metadata-update-type ::cs/file-metadata-sharing-updated
                     ::md/sharing-update ::md/sharing-conj
                     ::md/id uid
                     ::md/activity ::md/meta-read
                     :kixi.group/id new-group})
        (wait-is= updated-metadata
                  (get-metadata-by-id uid new-group))
        (wait-is= updated-metadata
                  (get-metadata-by-id uid uid)))

      (testing "Random group can't access"
        (always-is= nil
                    (get-metadata-by-id uid (uuid))))

      (testing "Group access revokable"
        (send-event {::cs/file-metadata-update-type ::cs/file-metadata-sharing-updated
                     ::md/id uid
                     ::md/sharing-update ::md/sharing-disj
                     ::md/activity ::md/meta-read
                     :kixi.group/id uid})
        (wait-is= (update-in updated-metadata
                             [::md/sharing ::md/meta-read]
                             #(into [] (remove #{uid} %)))
                  (get-metadata-by-id uid new-group))
        (always-is= nil
                    (get-metadata-by-id uid uid))))))


;; bundles

(defn create-bundle-payload
  [user-id bundled-ids & [overrides]]
  (merge-with merge
              {::cs/file-metadata-update-type ::cs/file-metadata-created
               ::md/file-metadata {::md/type "bundle"
                                   ::md/bundle-type "datapack"
                                   ::md/name "Test Bundle"
                                   ::md/file-type "csv"
                                   ::md/id user-id
                                   ::md/bundle-ids bundled-ids
                                   ::md/provenance {::md/source "upload"
                                                    :kixi.user/id user-id
                                                    ::md/created (conformers/time-unparser (t/now))}
                                   ::md/size-bytes 10
                                   ::md/sharing {::md/file-read [user-id]
                                                 ::md/meta-update [user-id]
                                                 ::md/meta-read [user-id]}}}
              (or overrides {})))

(defn delete-bundle
  [comms id]
  (kcomms/send-valid-event! comms
                            {::event/type :kixi.datastore/bundle-deleted
                             ::event/version "1.0.0"
                             ::md/id id}
                            {:kixi.comms.event/partition-key id}))

(deftest create-bundle-search
  (let [comms (:communications @user/system)
        uid (uuid)
        bundled-ids (into [] (repeatedly 3 uuid))
        send-event (partial send-event comms uid)
        bundle-payload (create-bundle-payload uid bundled-ids)]
    (testing "Bundle creatable and searchable"
      (send-event bundle-payload)
      (wait-is= (::md/file-metadata bundle-payload)
                (get-metadata-by-id uid))
      (wait-is= (::md/file-metadata bundle-payload)
                (first-item (search-metadata uid "Test Bundle"))))
    (testing "Delete bundle"
      (delete-bundle comms uid)
      (wait-is= nil
                (first-item (search-metadata uid "Test Bundle"))))))