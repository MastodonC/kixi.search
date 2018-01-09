(ns kixi.search.web
  (:require [byte-streams :as bs]
            [clojure.edn :as edn]
            [cheshire.core :as json]
            [clojure.spec.alpha :as spec]
            [com.walmartlabs.lacinia.util :refer [attach-resolvers]]
            [com.walmartlabs.lacinia :refer [execute]]
            [com.walmartlabs.lacinia.schema :as schema]
            [bidi
             [bidi :refer [tag]]
             [ring :refer [make-handler]]
             [vhosts :refer [vhosts-model]]]
            [com.stuartsierra.component :as component]
            [kixi.search.graphql :refer [namespaced-map->graphql-map
                                         graphql-map->namespaced-map
                                         graphql-keyword->keyword
                                         keyword->graphql-keyword
                                         assoc-in-queries]]
            [kixi.search.elasticsearch.query :as es]
            [kixi.spec :refer [alias]]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [medley.core :refer [map-vals]]
            [clojure.java.io :as io]
            [ring.adapter.jetty :refer [run-jetty]]
            [ring.util.response :refer [not-found response status]]
            [ring.util.request :refer [body-string]]
            [ring.middleware.json :refer [wrap-json-response]]
            [clojure.spec.alpha :as s]
            [kixi.search.elasticsearch.query :as query]))

(defn healthcheck
  []
  ;;Return truthy for now, but later check dependancies
  (response
   {:body "All is well"}))

(defn vec-if-not
  [x]
  (if (or (nil? x)
          (vector? x))
    x
    (vector x)))

(defn request->user-groups
  [request]
  (some-> (get-in request [:headers "user-groups"])
          (clojure.string/split #",")
          vec-if-not))

(defn prn-t
  [x]
  (prn x)
  x)

(def queryable-string string?)

;; TODO generate these

(alias 'ms 'kixi.datastore.metadatastore)
(alias 'msq 'kixi.datastore.metadatastore.query)

(spec/def ::msq/name string?)
(spec/def ::msq/description string?)
(spec/def ::meta-read list?)
(spec/def ::msq/sharing
  (spec/keys :opt-un [::meta-read]))

(spec/def ::query
  (spec/and (spec/keys :opt [::msq/name
                             ::msq/description])
            (spec/every-kv #{::msq/name
                             ::msq/description}
                           (constantly true))))

(spec/def ::filter
  (spec/and (spec/keys :opt [
                             ::msq/sharing])
            (spec/every-kv #{
                             ::msq/sharing}
                           (constantly true))))

(spec/def ::query-map
  (spec/or :nil nil?
           :query (spec/keys :opts-un [::query ::filter])))

(defn file-meta
  [query]
  (fn [& args]
    (prn args)))

(defn namespaced-keyword
  [s]
  (let [splits (clojure.string/split s #"/")]
    (if (second splits)
      (keyword (first splits) (second splits))
      (keyword s))))

(defn ensure-group-access
  [request-groups sharing-filter]
  (let [users-groups (set request-groups)]
    (as-> (or sharing-filter {}) $
      (map-vals (comp vec (partial filter users-groups)) $)
      (update $ ::ms/meta-read
              #(or % (vec users-groups))))))

(defn metadata-query
  [query]
  (fn [request]
    (let [query-raw (json/parse-string (body-string request)
                                       namespaced-keyword)
          conformed-query (spec/conform ::query-map
                                        query-raw)]
      (if-not (= ::spec/invalid conformed-query)
        (prn-t (response
                (query/find-by-query query
                                     (update-in (spec/unform ::query-map conformed-query)
                                                [:filter ::msq/sharing]
                                                (partial ensure-group-access (request->user-groups request)))
                                     0 ;;from-index
                                     10 ;;cnt
                                     ["kixi.datastore.metadatastore/id"] ;;sort-by
                                     :asc ;;sort-order
                                     )))
        {:status 400
         :body (do (spec/explain ::query-map query-raw)
                   (spec/explain-data ::query-map query-raw))}))))

(defn routes
  "Create the URI route structure for our application."
  [query]
  [""
   [["/healthcheck" healthcheck]
    ["/metadata" {:get [[["/" :id] (file-meta query)]]
                  ;; TODO add content-type guard
                  :post [[[""] (metadata-query query)]]}]

    [true (constantly (not-found nil))]]])

(defrecord Web
    [port ^org.eclipse.jetty.server.Server jetty query]
  component/Lifecycle
  (start [component]
    (if-not jetty
      (do
        (infof "Starting web-server on port %s" port)
        (assoc component
               :jetty
               (run-jetty
                ;; TODO Add json middleware
                (->> query
                     routes
                     make-handler
                     wrap-json-response)
                {:port port
                 :join? false})))
      component))
  (stop [component]
    (if jetty
      (do
        (infof "Stopping web-server on port")
        (.stop jetty)
        (dissoc component :jetty))
      component)))
