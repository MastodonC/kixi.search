(ns kixi.search.web
  (:require [byte-streams :as bs]
            [clojure.edn :as edn]
            [cheshire.core :as json]
            [com.walmartlabs.lacinia.util :refer [attach-resolvers]]
            [com.walmartlabs.lacinia :refer [execute]]
            [com.walmartlabs.lacinia.schema :as schema]
            [bidi
             [bidi :refer [tag]]
             [vhosts :refer [vhosts-model]]]
            [com.stuartsierra.component :as component]
            [kixi.search.graphql :refer [namespaced-map->graphql-map
                                         graphql-map->namespaced-map
                                         graphql-keyword->keyword
                                         keyword->graphql-keyword
                                         assoc-in-queries]]
            [kixi.search.elasticsearch.query :as es]
            [taoensso.timbre :as timbre :refer [error info infof]]
            [yada
             [resource :as yr]
             [yada :as yada]]
            [clojure.java.io :as io]))


(defn healthcheck
  [ctx]
                                        ;Return truthy for now, but later check dependancies
  (assoc (:response ctx)
         :status 200
         :body "All is well"))

(defn vec-if-not
  [x]
  (if (or (nil? x)
          (vector? x))
    x
    (vector x)))

(defn ctx->user-groups
  [ctx]
  (some-> (get-in ctx [:request :headers "user-groups"])
          (clojure.string/split #",")
          vec-if-not))

(defn prn-t
  [x]
  (prn x)
  x)

(defn graphql
  [schema ctx]
  {:status 200
   :headers {"Content-Type" "application/json"}
   :body (let [body-stream (get-in ctx [:body])
               groups (ctx->user-groups ctx)
               result (execute schema
                               (bs/to-string body-stream)
                               nil
                               {:groups groups})]
           (json/generate-string result))})

(defn routes
  "Create the URI route structure for our application."
  [schema]
  [""
   [["/healthcheck" healthcheck]
    ["/graphql" (partial graphql schema)]

    ;; This is a backstop. Always produce a 404 if we get there. This
    ;; ensures we never pass nil back to Aleph.
    [true (yada/handler nil)]]])

(defn wrap-resolver
  [f]
  (fn [context arguments value]
    (let [args (graphql-map->namespaced-map arguments)
          results (f context args value)]
      (map namespaced-map->graphql-map
           results))))

(defn get-sharing-matrix
  [context arguments value]
  (let [activity (graphql-keyword->keyword (get-in context [:com.walmartlabs.lacinia/selection :field]))]
    (prn value)
    (get value
         (prn-t
          (keyword (str "kixi_datastore_metadatastore___" (name (keyword->graphql-keyword activity))))))))

(defn compile-schema
  [query]
  (-> "schema.edn"
      io/resource
      io/file
      slurp
      edn/read-string
      namespaced-map->graphql-map
      prn-t
      (attach-resolvers {:get-metadata (wrap-resolver
                                        (fn [context args value]
                                          [(es/find-by-id query (:kixi.datastore.metadatastore/id args))]))
                         :get-sharing-matrix get-sharing-matrix})
      schema/compile))

(defrecord Web
    [port listener query]
  component/Lifecycle
  (start [component]
    (if listener
      component
      (let [_ (infof "Starting web-server on port %s" port)
            listener (yada/listener (routes (compile-schema query))
                                    {:port port})]
        (assoc component :listener listener))))
  (stop [component]
    (when-let [close (get-in component [:listener :close])]
      (infof "Stopping web-server on port %s" port)
      (close))
    (assoc component :listener nil)))
