(ns kixi.search.web
  (:require [bidi.bidi :refer [tag]]
            [bidi.ring :refer [make-handler]]
            [bidi.vhosts :refer [vhosts-model]]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as spec]
            [com.rpl.specter :as specter :refer [ALL MAP-VALS STAY]]
            [com.stuartsierra.component :as component]
            [kixi.search.metadata.query :as query]
            [kixi.search.query-model :as model]
            [kixi.spec :refer [alias]]
            [medley :refer [map-vals]]
            [ring.adapter.jetty :refer [run-jetty]]
            [ring.middleware.json :refer [wrap-json-response]]
            [ring.util.request :refer [body-string]]
            [ring.util.response :refer [not-found response status]]
            [taoensso.timbre :as timbre :refer [error info infof]]))

(defn healthcheck
  [_]
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

(alias 'ms 'kixi.datastore.metadatastore)
(alias 'msq 'kixi.datastore.metadatastore.query)

(defn ensure-group-access
  [request-groups sharing-filter]
  (let [users-groups (set request-groups)]
    (as-> (or sharing-filter {}) $
      (specter/transform
       [MAP-VALS MAP-VALS]
       (partial filter users-groups)
       $)
      (update-in $
                 [::msq/meta-read :contains]
                 #(or % (vec users-groups))))))

(def EVERYTHING
  (specter/recursive-path
   [] p
   (specter/if-path vector?
                    [ALL p]
                    STAY)))

(defn parse-nested-vectors
  [unparsed]
  (specter/transform
   [EVERYTHING]
   namespaced-keyword
   unparsed))

(defn metadata-query
  [query]
  (fn [request]
    (let [query-raw (update (json/parse-string (body-string request)
                                               namespaced-keyword)
                            :fields parse-nested-vectors
                            :sort-by parse-nested-vectors)
          conformed-query (spec/conform ::model/query-map
                                        query-raw)]
      ;;TODO user-groups header must be present
      (if-not (= ::spec/invalid conformed-query)
        (response
         (query/find-by-query query
                              (update-in (or (:query (apply hash-map conformed-query)) {})
                                         [:query ::msq/sharing]
                                         (partial ensure-group-access (request->user-groups request)))))
        {:status 400
         :body (spec/explain-data ::model/query-map query-raw)}))))

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
