(ns kixi.search-test
  (:require [clojure.test :refer :all]
            [clj-time.core :as t]
            [kixi.datastore.metadatastore :as md]
            [kixi.spec.conformers :as conformers]
            [kixi.user :as user]
            [kixi.search.elasticsearch.event-handlers.metadata-create :as es]))

(comment "A large test suite that generates a fixed sequence of datastore events to build up a prod like dataset,
Then performs various searches to explore the backends and how they perform.

Spike work")

(defn uuid
  []
  (str (java.util.UUID/randomUUID)))

(def users-num 100)
(def files-per-user 10)
(def files-shared-with-next 10)

(defn create-file-event
  [user next-users file-index]
  {::md/type "stored"
   ::md/file-type "csv"
   ::md/id (uuid)
   ::md/name (str "Test File " file-index "for " uuid)
   ::md/provenance {::md/source "upload"
                    ::user/id user
                    ::md/created (conformers/time-unparser (t/now))}
   ::md/size-bytes 10
   ::md/sharing {::md/file-read (cons user next-users)
                 ::md/meta-update (cons user next-users)}})

(defn dataset-events
  ([] (dataset-events (repeatedly users-num uuid)))
  ([users]
   (when users
     (lazy-cat (map
                (partial create-file-event
                         (first users)
                         (take files-shared-with-next users))
                (range 0 files-per-user))
               (dataset-events (seq (rest users)))))))
