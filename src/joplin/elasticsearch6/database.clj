(ns joplin.elasticsearch6.database
  (:require [clj-time
             [core :as t]
             [format :as f]]
            [clj-http.client :as client]
            [joplin.core :refer [migrate-db
                                 rollback-db
                                 seed-db
                                 pending-migrations
                                 create-migration
                                 get-migrations
                                 do-migrate
                                 do-rollback
                                 do-seed-fn
                                 do-pending-migrations
                                 do-create-migration]]
            [cheshire.core :as json]
            [ragtime.protocols :refer [DataStore]]))

(def default-migration-index "migrations")
(def migration-type "migration")
(def migration-document-id "0")

(defn- get-migration-index [db]
  (or (:migration-index db) default-migration-index))

(defn kw->es-format
  [kw]
  (if (qualified-keyword? kw)
    (str (clojure.string/replace (namespace kw) "." "_")
         "__"
         (name kw))
    (name kw)))

(defn es-format->kw
  [confused-kw]
  (let [splits (clojure.string/split (name confused-kw) #"__")]
    (if (second splits)
      (keyword
       (clojure.string/replace (first splits) "_" ".")
       (second splits))
      confused-kw)))

(defn map-all-keys
  [f]
  (fn mapper [m]
    (cond
      (map? m) (zipmap (map f (keys m))
                       (map mapper (vals m)))
      (list? m) (map mapper m)
      (vector? m) (mapv mapper m)
      (seq? m) (str m) ; (map mapper m) spec errors contain seq's of sexp's containing code, which breaks elasticsearch validation.
      (symbol? m) (name m)
      (keyword? m) (f m)
      :else m)))

(defn kw->es-format
  [kw]
  (if (qualified-keyword? kw)
    (str (clojure.string/replace (namespace kw) "." "_")
         "__"
         (name kw))
    (name kw)))

(defn es-format->kw
  [confused-kw]
  (let [splits (clojure.string/split (name confused-kw) #"__")]
    (if (second splits)
      (keyword
       (clojure.string/replace (first splits) "_" ".")
       (second splits))
      confused-kw)))


(def all-keys->es-format
  (map-all-keys kw->es-format))

(def all-keys->kw
  (map-all-keys es-format->kw))

(defn insert
  [es-url index-name doc-type id document]
  (client/put (str es-url "/" index-name "/" doc-type "/" id)
              {:body (json/generate-string
                      (all-keys->es-format document))
               :as :json
               :headers {:content-type "application/json"}}))

(defn index-exists?
  [es-url index-name]
  (-> (str es-url "/" index-name)
      (client/head {:throw-exceptions false})
      :status
      (= 200)))

(defn create-index
  [es-url index-name]
  (client/put (str es-url "/" index-name)))

(defn get-data
  [es-url index-name doc-type id]
  (let [resp (client/get
              (str es-url "/" index-name "/" doc-type "/" id)
              {:throw-exceptions false})]
    (when (= 200 (:status resp))
      (-> (:body resp)
          (json/parse-string keyword)
          :_source
          all-keys->kw))))

(defn- ensure-migration-index
  [es-url migration-index]
  (when-not (index-exists? es-url migration-index)
    (create-index es-url
                  migration-index)))

(defn- timestamp-as-string
  []
  (f/unparse (f/formatters :date-time) (t/now)))

(defn- es-get-applied
  [es-url migration-index]
  (ensure-migration-index es-url migration-index)
  (some->> (get-data es-url
                     migration-index
                     migration-type
                     migration-document-id)
           :_source
           :migrations
           es-format->kw))

(defn es-add-migration-id
  [es-url migration-index migration-id]
  (insert es-url
          migration-index
          migration-type
          migration-document-id
          {:migrations
           (assoc
            (es-get-applied es-url migration-index)
            migration-id
            (timestamp-as-string))}))

(defn es-remove-migration-id
  [es-url migration-index migration-id]
  (insert es-url
          migration-index
          migration-type
          migration-document-id
          {:migrations
           (dissoc
            (es-get-applied es-url migration-index)
            migration-id
            (timestamp-as-string))}))

(defn es-get-applied-migrations
  [es-client migration-index]
  (or (some->> (es-get-applied es-client migration-index)
               (sort-by second)
               keys
               (map name))
      []))

(defn es-url
  [protocol host port]
  (str protocol "://" host ":" port))

(defrecord ES6Database
    [protocol host port index migration-index cluster]
  DataStore
  (add-migration-id [db id]
    (es-add-migration-id (es-url protocol host port)
                         (get-migration-index db) id))
  (remove-migration-id [db id]
    (es-remove-migration-id (es-url protocol host port)
                            (get-migration-index db) id))
  (applied-migration-ids [db]
    (es-get-applied-migrations (es-url protocol host port)
                               (get-migration-index db))))


(defmethod migrate-db :es6 [target & args]
  (apply do-migrate (get-migrations (:migrator target))
         (map->ES6Database (:db target)) args))

(defmethod rollback-db :es6 [target amount-or-id & args]
  (apply do-rollback (get-migrations (:migrator target))
         (->ES6Database target) amount-or-id args))

(defmethod seed-db :es6 [target & args]
  (apply do-seed-fn (get-migrations (:migrator target))
         (->ES6Database target) target args))

(defmethod pending-migrations :es6 [target & args]
  (do-pending-migrations (->ES6Database target)
                         (get-migrations (:migrator target))))

(defmethod create-migration :es6 [target id & args]
  (do-create-migration target id "joplin.elasticsearch6.database"))
