(ns kixi.search.elasticsearch
  (:require [cheshire.core :as json]
            [clojurewerkz.elastisch.native :as esr]
            [clojurewerkz.elastisch.native
             [document :as esd]
             [response :as esrsp]]
            [environ.core :refer [env]]
            [joplin.repl :as jrepl]
            [kixi.search.time :as t]
            [taoensso.timbre :as timbre :refer [error info]]))

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
  {:type "string"
   :store "yes"
   :index "not_analyzed"})

(def string-analyzed
  {:type "string"
   :store "yes"})

(def timestamp
  {:type "date"
   :format t/es-format})

(def long
  {:type "long"})

(def double
  {:type "double"})

(defn kw->es-format
  [kw]
  (if (namespace kw)
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

(def all-keys->es-format
  (map-all-keys kw->es-format))

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
      (if (map? v)
        (merge a
               (collapse-nesting v (str prefix k ".")))
        (assoc a (str prefix k)
               v)))
    {}
    m)))

(defn query->es-filter
  [query]
  {:filter
   {:bool
    {:must
     (map
      (fn [[k values]]
        {:terms {k values}})
      (collapse-nesting
       (all-keys->es-format
        query)))}}})

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

(defn search-data
  [index-name doc-type conn query from-index cnt sort-by sort-order]
  (try
    (let [resp (esd/search conn
                           index-name
                           doc-type
                           (merge (query->es-filter query)
                                  {:from from-index
                                   :size cnt
                                   :sort {(sort-by-vec->str sort-by)
                                          {:order sort-order}}}))]
      {:items (doall
               (map (comp all-keys->kw :_source)
                    (esrsp/hits-from resp)))
       :paging {:total (esrsp/total-hits resp)
                :count (count (esrsp/hits-from resp))
                :index from-index}})
    (catch Exception e
      (error e)
      (throw e))))
