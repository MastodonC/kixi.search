(ns kixi.search.elasticsearch.client
  (:require [cheshire.core :as json]
            [clj-http.client :as client]
            [clojure.spec.alpha :as spec]
            [clojurewerkz.elastisch.native :as esr]
            [clojurewerkz.elastisch.native.document :as esd]
            [com.rpl.specter :as specter]
            [environ.core :refer [env]]
            [joplin.repl :as jrepl]
            [kixi.search.query-model :as model]
            [kixi.search.time :as t]
            [medley :refer [map-vals]]
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
  {:type "keyword"
   :store "true"
   :index "true"})

(def string-analyzed
  {:type "text"
   :store "true"
   :index "true"})

(def string-autocomplete
  {:type "text"
   :store "true"
   :index "true"
   :analyzer "autocomplete"})

(def timestamp
  {:type "date"
   :format t/es-format})

(def long
  {:type "long"})

(def double
  {:type "double"})

(def boolean
  {:type "boolean"})

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
               :as :json
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
               (or (every? map? (vals v))
                   (every? (comp model/sort-orders keyword) (vals v))))
        (merge a
               (collapse-nesting v (str prefix k ".")))
        (assoc a (str prefix k)
               v)))
    {}
    m)))

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
     #(get % k) $)
    (when-not (empty? $) $)))

(defn field-vectors->collapsed-es-fields
  [kw-or-vec]
  (cond
    (vector? kw-or-vec) (->> kw-or-vec
                             (mapv kw->es-format)
                             (clojure.string/join "."))
    (keyword? kw-or-vec) (kw->es-format kw-or-vec)))

(defn sort-by->collapsed-es-sorts
  [sort-def]
  (cond
    (keyword? sort-def) {sort-def "asc"}
    (map? sort-def) (->> sort-def
                         (map-vals (some-fn model/sort-orders sort-by->collapsed-es-sorts))
                         all-keys->es-format
                         collapse-nesting)))

(defn query->es-filter
  [{:keys [query fields sort-by from size] :as query-map}]
  (let [flat-query (collapse-nesting
                    (all-keys->es-format
                     query))]
    (merge {:query
            {:bool
             (merge {:filter (merge (when-let [contains (select-nested
                                                         flat-query
                                                         "contains")]
                                      {:terms contains})
                                    (when-let [equals (select-nested
                                                       flat-query
                                                       "equals")]
                                      {:term equals}))}
                    (when-let [matchers (select-nested
                                         flat-query
                                         "match")]
                      {:must {:match matchers}}))}}
           (when-not (empty? fields)
             {:_source (mapv field-vectors->collapsed-es-fields fields)})
           (when-not (empty? sort-by)
             {:sort (mapv sort-by->collapsed-es-sorts sort-by)})
           (when from
             {:from from})
           (when size
             {:size (min size 100)}))))

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

(defn get-by-id
  [index-name doc-type es-url id]
  (let [resp (client/get (str es-url "/" index-name "/" doc-type "/" id)
                         {:throw-exceptions false})]
    (when (= 200 (:status resp))
      (-> (:body resp)
          (json/parse-string keyword)
          :_source
          all-keys->kw))))

(defn search
  [es-url index-name doc-type query]
  (client/get
   (str es-url "/" index-name "/" doc-type "/_search")
   {:body (json/generate-string query)
    :headers {:content-type "application/json"}}))

(spec/fdef search-data
           :args (spec/cat :index-name string?
                           :doc-type string?
                           :conn string?
                           :query-map ::model/query-map))

(defn search-data
  [index-name doc-type conn query-map]
  (try
    (let [resp (json/parse-string
                (:body (search conn
                               index-name
                               doc-type
                               (query->es-filter query-map)))
                keyword)]
      {:items (mapv (comp all-keys->kw (some-fn :_source :fields))
                    (get-in resp [:hits :hits]))
       :paging {:total (get-in resp [:hits :total])
                :count (count (get-in resp [:hits :hits]))
                :index (:from query-map 0)}})
    (catch Exception e
      (error e)
      (throw e))))

(defn index-exists?
  [es-url index-name]
  (-> (str es-url "/" index-name)
      (client/head {:throw-exceptions false})
      :status
      (= 200)))

(defn create-index-
  [es-url index-name definition]
  (client/put (str es-url "/" index-name)
              {:body (json/generate-string definition)
               :headers {:content-type "application/json"}}))

(defn create-index
  [es-url index-name doc-type definition]
  (create-index- es-url
                 index-name
                 {:mappings {doc-type
                             {:properties (all-keys->es-format definition)}}
                  :settings {:number_of_shards 5
                             :analysis {:filter {:autocomplete_filter
                                                 {:type "edge_ngram"
                                                  :min_gram 1 ;; Might want this to be 2
                                                  :max_gram 20}}
                                        :analyzer {:autocomplete
                                                   {:type "custom"
                                                    :tokenizer "standard"
                                                    :filter ["lowercase"
                                                             "autocomplete_filter"]}}}}}))
