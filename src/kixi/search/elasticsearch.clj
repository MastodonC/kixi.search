(ns kixi.search.elasticsearch
  (:require [cheshire.core :as json]
            [clojurewerkz.elastisch.native :as esr]
            [clojurewerkz.elastisch.native
             [document :as esd]
             [response :as esrsp]]
            [clj-http.client :as client]
            [environ.core :refer [env]]
            [joplin.repl :as jrepl]
            [kixi.search.time :as t]
            [taoensso.timbre :as timbre :refer [error info]]
            [com.rpl.specter :as specter]))

(def put-opts (merge {:consistency (env :elasticsearch-consistency "default")
                      :replication (env :elasticsearch-replication "default")
                      :refresh (Boolean/parseBoolean (env :elasticsearch-refresh "false"))}
                     (when-let [s  (env :elasticsearch-wait-for-active-shards nil)]
                       {:wait-for-active-shards s})))

(defn migrate
  [env migration-conf]
  (->>
   (with-out-str
     (jrepl/migrate migration-conf env))
   (clojure.string/split-lines)
   (run! #(info "JOPLIN:" %))))

(def string-stored-not_analyzed
  {:type "keyword"
   :store "true"
   :index "true"})

(def string-analyzed
  {:type "text"
   :store "true"
   :index "true"})

(def timestamp
  {:type "date"
   :format t/es-format})

(def long
  {:type "long"})

(def double
  {:type "double"})

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


(defn remove-query-sub-ns
  [k]
  (if (qualified-keyword? k)
    (keyword
     (first (clojure.string/split (namespace k)
                                  #".query"))
     (name k))
    k))

(def all-keys->es-format
  (map-all-keys (comp kw->es-format remove-query-sub-ns)))

(def all-keys->es-format-kws
  (map-all-keys (comp keyword kw->es-format)))

(def all-keys->kw
  (map-all-keys es-format->kw))

(defn get-document-raw
  [index-name doc-type conn id]
  (esd/get conn
           index-name
           doc-type
           id
           {:preference "_primary"}))

(defn get-document
  [index-name doc-type conn id]
  (-> (get-document-raw index-name doc-type conn id)
      :_source
      all-keys->kw))

(defn get-document-key
  [index-name doc-type conn id k]
  (-> (get-document-raw index-name doc-type conn id)
      :_source
      (get (keyword (kw->es-format k)))
      all-keys->kw))

(def apply-attempts 10)

(defn version-conflict?
  [resp]
  (some
   #(= "version_conflict_engine_exception"
       (:type %))
   ((comp :root_cause :error) resp)))

(defn error?
  [resp]
  (:error resp))

(defn apply-func
  ([index-name doc-type conn id f]
   (loop [tries apply-attempts]
     (let [curr (get-document-raw index-name doc-type conn id)
           resp (esd/put conn
                         index-name
                         doc-type
                         id
                         (f curr)
                         (merge put-opts
                                (when (:_version curr)
                                  {:version (:_version curr)})))]
       (if (and (version-conflict? resp)
                (pos? tries))
         (recur (dec tries))
         resp)))))

(defn merge-data
  [index-name doc-type conn id update]
  (let [es-u (all-keys->es-format update)
        r (apply-func
           index-name
           doc-type
           conn
           id
           (fn [curr]
             (merge-with merge
                         (:_source curr)
                         es-u)))]
    (if (error? r)
      (error "Unable to merge data for id: " id ". Trying to merge: " es-u ". Response: " r)
      r)))

(defn insert-data
  [index-name doc-type es-url id document]
  (client/put (str es-url "/" index-name "/" doc-type "/" id)
              {:body (json/generate-string (all-keys->es-format document))
               :headers {:content-type "application/json"}}))


(defn update-in-data
  [index-name doc-type conn id update-fn ks element]
  (let [r (apply-func
           index-name
           doc-type
           conn
           id
           (fn [curr]
             (update-in (:_source curr) (all-keys->es-format-kws ks)
                        #(vec (update-fn (set %) (all-keys->es-format-kws element))))))]
    (if (error? r)
      (error "Unable to cons data for id: " id ". Trying to update: " ks ". Response: " r)
      r)))

(defn present?
  [index-name doc-type conn id]
  (esd/present? conn index-name doc-type id))

(defn discover-executor
  [url]
  (let [cluster-info (json/parse-string (slurp url) keyword)]
    {:native-host-ports (->> cluster-info
                             (map :transport_address)
                             (map (comp rest #(re-find #"([0-9\.]+):([0-9]+)" %)))
                             (map (fn [[h p]] [h (Integer/parseInt p)])))
     :http-host-ports  (->> cluster-info
                            (map :http_address)
                            (map (comp rest #(re-find #"([0-9\.]+):([0-9]+)" %)))
                            (map (fn [[h p]] [h (Integer/parseInt p)])))}))

(defn connect
  [host-ports cluster]
  (esr/connect host-ports
               (merge {}
                      (when cluster
                        {:cluster.name cluster}))))

(def collapse-keys ["terms"])

(defn collapse-nesting
  ([m]
   (collapse-nesting m ""))
  ([m prefix]
   (reduce
    (fn [a [k v]]
      (if (and (map? v)
               (every? map? (vals v)))
        (merge a
               (collapse-nesting v (str prefix k ".")))
        (assoc a (str prefix k)
               v)))
    {}
    m)))

(defn prn-t
  [x]
  (prn x)
  x)

(defn submaps-with
  [nested-map k]
  (into {}
        (specter/select
         [specter/ALL
          (specter/selected? specter/LAST
                             (specter/must k))]
         nested-map)))

(defn select-nested
  [m k]
  (as-> m $
    (submaps-with $ k)
    (specter/transform
     [specter/MAP-VALS]
     #(get % k) $)))

(defn query->es-filter
  [{:keys [query] :as query-map}]
  (let [flat-query (collapse-nesting
                    (all-keys->es-format
                     query))]
    (prn "QM: " flat-query)
    (prn-t {:query
            {:bool
             ;; TODO :contains here should be query.model.array/contains
             {:filter {:terms (select-nested
                               (submaps-with flat-query "contains")
                               "contains")}}}})))

(defn str->keyword
  [^String s]
  (->> (clojure.string/split s #"/")
       (apply keyword)
       kw->es-format))

(defn sort-by-vec->str
  [sort-by-v]
  (->> sort-by-v
       (mapv str->keyword)
       (clojure.string/join ".")))

(defn search
  [es-url index-name doc-type query]
  (client/get
   (str es-url "/" index-name "/" doc-type "/_search")
   {:body (json/generate-string query)
    :headers {:content-type "application/json"}}))

(defn search-data
  [index-name doc-type conn query-map from-index cnt sort-by sort-order]
  (try
    (let [resp (prn-t (json/parse-string
                       (:body (search conn
                                      index-name
                                      doc-type
                                      (merge (query->es-filter query-map)
                                             {:from from-index
                                              :size cnt
                                              :sort {(sort-by-vec->str sort-by)
                                                     {:order sort-order}}})))
                       keyword))]
      {:items (mapv (comp all-keys->kw :_source)
                    (get-in resp [:hits :hits]))
       :paging {:total (get-in resp [:hits :total])
                :count (count (get-in resp [:hits :hits]))
                :index from-index}})
    (catch Exception e
      (error e)
      (throw e))))

(defn create-index
  [es-url index-name definition]
  (client/put (str es-url "/" index-name)
              {:body (json/generate-string definition)
               :headers {:content-type "application/json"}}))
