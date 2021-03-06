(ns kixi.search.elasticsearch.client
  (:require [cheshire.core :as json]
            [clj-http.client :as client]
            [clojure.spec.alpha :as spec]
            [com.rpl.specter :as specter]
            [environ.core :refer [env]]
            [joplin.repl :as jrepl]
            [kixi.datastore.metadatastore :as md]
            [kixi.search.query-model :as model]
            [kixi.search.time :as t]
            [medley :refer [map-vals remove-vals]]
            [taoensso.timbre :as timbre :refer [error info]]))

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
      (seq? m) (map mapper m)
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

(defn error?
  [resp]
  (:error resp))

(defn insert-data
  [index-name doc-type es-url id document]
  (client/put (str es-url "/" index-name "/" doc-type "/" id)
              {:body (json/generate-string (all-keys->es-format document))
               :as :json
               :headers {:content-type "application/json"}}))

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
    (keyword? sort-def) {(kw->es-format sort-def) "desc"}
    (map? sort-def) (->> sort-def
                         (map-vals (some-fn model/sort-orders sort-by->collapsed-es-sorts))
                         all-keys->es-format
                         collapse-nesting)))

(defn vector-when-multi
  [& args]
  (when-let [nilless (seq
                      (keep identity (flatten args)))]
    (if (< 1 (count nilless))
      (apply vector nilless)
      (first nilless))))

(defn wrap-query-type
  "Wraps each field query element in a map keyed by the query-type"
  [query-type query-elements]
  (mapv #(hash-map query-type (apply hash-map %)) query-elements))

(defn inject-standard-analyzer
  [query-term]
  (specter/transform
   [specter/MAP-VALS specter/MAP-VALS]
   #(hash-map :query %
              :analyzer
              "standard")
   query-term))

(def remove-empty
  (comp not-empty (partial remove-vals empty?)))

(defn query->es-filter
  [{:keys [query fields sort-by from size] :as query-map}]
  (let [flat-query (collapse-nesting
                    (all-keys->es-format
                     query))
        select-query (partial select-nested flat-query)]
    (merge {:query
            {:bool
             (merge (when-let [filters (vector-when-multi
                                           (when-let [contains (select-query "contains")]
                                             (wrap-query-type :terms contains))
                                         (when-let [equals (select-query "equals")]
                                           (wrap-query-type :term equals)))]
                      {:filter filters})
                    (when-let [matchers (remove-empty
                                         (select-query "match"))]
                      {:must (vector-when-multi
                                 (mapv
                                  inject-standard-analyzer
                                  (wrap-query-type :match matchers)))
                       :should (vector-when-multi
                                   (mapv
                                    #(hash-map :span_first
                                               {:match %
                                                :end 1})
                                    (wrap-query-type :span_term
                                                     matchers)))})
                    (when-let [exists (select-query "exists")]
                      (apply (partial merge-with vector)
                             (mapv
                              (fn [[field-name pred]]
                                (if pred
                                  {:must {:exists {:field field-name}}}
                                  {:must_not {:exists {:field field-name}}}))
                              exists))))}}
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

(defn get-by-id-raw-
  [index-name doc-type es-url id exceptions]
  (client/get (str es-url "/" index-name "/" doc-type "/" id)
              {:throw-exceptions exceptions}))

(defn ensure-set
  [x]
  (if (coll? x)
    (set x)
    (hash-set x)))

(def payload-fields-to-coerce
  {::md/tags set
   ::md/bundled-ids set})

(defn coerce-response
  [r fields]
  (clojure.walk/postwalk #(if (and (vector? %)
                                   (get fields (first %)))
                            (update % 1 ((first %) fields))
                            %) r))

(defn get-by-id
  [index-name doc-type es-url id groups]
  (let [resp (get-by-id-raw- index-name doc-type es-url id false)]
    (when (= 200 (:status resp))
      (let [item (-> (:body resp)
                     (json/parse-string keyword)
                     :_source
                     all-keys->kw
                     (coerce-response payload-fields-to-coerce))]
        (when (and (not (empty?
                          (clojure.set/intersection (ensure-set groups)
                                                    (set (get-in item [::md/sharing ::md/meta-read])))))
                   (not (::md/tombstone item)))
          item)))))

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
      {:items (mapv (comp #(coerce-response % payload-fields-to-coerce) all-keys->kw (some-fn :_source :fields))
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

(defn update-document-
  [es-url index-name doc-type id previous-version updated]
  (client/put (str es-url "/" index-name "/" doc-type "/" id)
              {:body (json/generate-string (all-keys->es-format updated))
               :query-params {:version previous-version}
               :headers {:content-type "application/json"}}))

(defn apply-func
  "Attempts to apply the supplied function to current version. Does not retry on concurrent failure, leaving that to users. The event partition, single thread processing model, should prevent these occuring"
  [index-name doc-type es-url id func]
  (let [raw-resp (get-by-id-raw- index-name doc-type es-url id false)
        decoded-resp (-> (:body raw-resp)
                         (json/parse-string keyword)
                         all-keys->kw)
        new-value (func (:_source decoded-resp))]
    (update-document- es-url index-name doc-type id (:_version decoded-resp) new-value)))
