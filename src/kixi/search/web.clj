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

(defn file-meta
  [query]
  (fn [request]
    (let [meta-id (get-in request [:params :id])
          user-groups (request->user-groups request)
          meta (query/find-by-id query
                                 meta-id
                                 user-groups)]
      (if meta
        (response meta)
        {:status 404}))))

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
  (when unparsed
    (specter/transform
     [EVERYTHING]
     namespaced-keyword
     unparsed)))


(defn parse-sort-by
  [unparsed]
  (when unparsed
    (mapv
     (fn [element]
       (if (map? element)
         (zipmap (keys element)
                 (parse-sort-by (vals element)))
         (namespaced-keyword element)))
     unparsed)))

(defn update-present
  [m k f]
  (if (k m)
    (update m k f)
    m))

(defn metadata-query
  [query]
  (fn [request]
    (let [query-raw (-> (json/parse-string (body-string request)
                                           namespaced-keyword)
                        (update-present :fields parse-nested-vectors)
                        (update-present :sort-by parse-sort-by))
          conformed-query (spec/conform ::model/query-map
                                        query-raw)]
      (cond (= ::spec/invalid conformed-query)
            {:status 400
             :body (spec/explain-data ::model/query-map query-raw)}

            (empty? (request->user-groups request))
            {:status 400
             :body {:reason "no-user-groups"}}

            :else
            (response
             (query/find-by-query query
                                  (merge-with merge
                                              {:query {::msq/tombstone {:exists false}}}
                                              (update-in (or (:query (apply hash-map conformed-query)) {})
                                                         [:query ::msq/sharing]
                                                         (partial ensure-group-access (request->user-groups request))))))))))

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
    [port ^org.eclipse.jetty.server.Server jetty metadata-query]
  component/Lifecycle
  (start [component]
    (if-not jetty
      (do
        (infof "Starting web-server on port %s" port)
        (assoc component
               :jetty
               (run-jetty
                ;; TODO Add json middleware
                (->> metadata-query
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
