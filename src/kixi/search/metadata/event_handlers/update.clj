(ns kixi.search.metadata.event-handlers.update
  (:require [com.stuartsierra.component :as component]
            [clojure.spec.alpha :as s]
            [kixi.comms :as c]
            [kixi.datastore.metadatastore :as md]
            [kixi.search.elasticsearch.client :as es]
            [kixi.spec :refer [alias]]
            [taoensso.timbre :as timbre :refer [info]]))

(alias 'cs 'kixi.datastore.communication-specs)

(def base-index-name "kixi-search_metadata")
(def doc-type "metadata")

(defmethod c/event-payload
  [:kixi.datastore/sharing-changed "1.0.0"]
  [_]
  (s/keys :req [::md/id
                ::md/sharing-update
                :kixi.group/id
                ::md/activity]))

(defmulti update-metadata-processor
  (fn [es-url index-name update-event]
    (::cs/file-metadata-update-type update-event)))

(defmethod update-metadata-processor ::cs/file-metadata-created
  [es-url index-name update-event]
  (let [metadata (::md/file-metadata update-event)]
    (info "Create: " metadata)
    (es/insert-data
     index-name
     doc-type
     es-url
     (::md/id metadata)
     metadata)))

(defmethod update-metadata-processor ::cs/file-metadata-structural-validation-checked
  [es-url index-name update-event]
  (info "Update: " update-event))


(defn sharing-updater
  [current update-event]
  (let [group-id (:kixi.group/id update-event)
        update-fn (case (::md/sharing-update update-event)
                    ::md/sharing-conj #(into [] (cons group-id %))
                    ::md/sharing-disj #(into [] (remove #{group-id} %)))]
    (update-in current
               [::md/sharing (::md/activity update-event)]
               update-fn)))

(defmethod update-metadata-processor ::cs/file-metadata-sharing-updated
  [es-url index-name update-event]
  (info "Update Share: " update-event)
  (es/apply-func index-name doc-type es-url
                 (::md/id update-event)
                 #(sharing-updater % update-event)))

(defn sharing-change-processor
  [es-url index-name update-event]
  (info "Update Share: " update-event)
  (es/apply-func index-name doc-type es-url
                 (::md/id update-event)
                 #(sharing-updater % update-event)))

(defn dissoc-nonupdates
  [md]
  (reduce
   (fn [acc [k v]]
     (if (and (namespace k) (clojure.string/index-of (namespace k) ".update"))
       (assoc acc k v)
       acc))
   {}
   md))

(defn- update?
  [kw]
  (and (qualified-keyword? kw)
       (clojure.string/index-of (namespace kw) ".update")))

(defn remove-update-ns
  [kw]
  (when (update? kw)
    (keyword
     (subs (namespace kw)
           0
           (clojure.string/index-of (namespace kw) ".update"))
     (name kw))))

;; The update command DSL, by example, looks like this.
;;
;; {:set #{"baz"}
;;  :conj #{"foo" "bar"}
;;  :disj #{"bar"}
;; }
;;
;; or simply
;;
;; :rm
;;
;; In practice however the DSL is limited to a single operator e.g.
;;
;; {:conj #{"foo"}}
;;
;; Now it looks like an s-expressions with list syntax replaced by map
;; syntax and the operaters one or more of the three keywords you see here.
;; The operator is often referred to as either a "command" or
;; an "action" over the Witan code bases.
;;
;; The :conj and :disj  DSL operaters are confusing, they are not:
;; - conj(oin) or disj(oin) from the clojure world
;; - nor are they conj(unction) or disj(unction) from the maths world
;; Rather conj is closet to set union and disj is set difference in mathematics.
;; Or in the clojure world conj is a concat and disj is a remove with a set function. 
;;
;; As, ever things are more complicated.  There are multiple DSL's -
;; one for each and every meta-data field that allows updates.
;;
;; ::md.update/tags {<:conj|:disj> #{"element1" "element2" ...}}
;; ::md.update/name {:set "string_name"}
;;
;; The metadata field has its namespace postfixed by an .update.  In
;; the pseudo code above we see tags allows either :conj or :disj as
;; the operator and name allows only the :set operator i.e. different
;; DSL's.
;;
;; Further the DSL argument is defined by the spec of the metadata
;; field. e.g., for ::md/tags.update the arguments for :conj or :disj
;; has to conform to the spec of ::md/tags (no .update).  So in these
;; specific examples the arguments for tags has to be a set and the
;; arguments for name has to be as string.  This is convenient for
;; generation of specs but awkward to work with as writing something like
;;
;; ::md.update/tags {:conj "new_tag"}
;;
;; is very natural but unfortunately disallowed.  Instead you have to write
;;
;; ::md.update/tags {:conj #("new_tag")}
;;
;; The implementation of the parser for the DSL does not 
;;
;; The datastore link here
;; provides some but not all of the DSL definitions for the metadata
;; fields.
;;
;; https://github.com/mastodonc/kixi.datastore/blob/master/src/kixi/datastore/metadatastore/command_handler.clj#L255
;;
(def update-dsl-cmds #{:set :conj :disj})

;; technically the update DSL may have more than one expression
;; but in practice they don't
(defn- update-dsl?
  [x]
  (and (map? x)
       (= 1 (count (keys x)))
       (update-dsl-cmds (first (keys x)))))

(def get-update-dsl-cmd (comp first keys))
(def get-update-dsl-arg (comp first vals))

(declare apply-updates)

(defn- to-seq
  [x]
  (cond
    (coll? x) x
    (nil? x) x
    :else (vector x)))

(defn apply-update
  [current kw update-dsl]
  (if (= :rm update-dsl)
    (dissoc current
            (remove-update-ns kw))
    ;;update-fn is a var set to a function depending on the value of a metadata field
    ;;If it's an update DSL then parse and return a function that will apply it.
    ;;Else recurse into the value looking for nested updates.
    (let [update-fn (cond
                      (update-dsl? update-dsl)
                      (let [cmd (get-update-dsl-cmd update-dsl) ;; we only deal with single line update DSLs
                            arg (get-update-dsl-arg update-dsl)]
                        (case cmd
                          :set (constantly arg)
                          :conj #(distinct (concat (to-seq %) (to-seq arg)))
                          :disj #(remove (set (to-seq arg)) (to-seq %))))
                      :else 
                      #(apply-updates % update-dsl))]
      (update current
              (remove-update-ns kw)
              update-fn))))

(defn apply-updates
  "Takes the current backend representation of the metadata as the
  initial value of a reduce applying updates over the metadata
  fields.  The updates are of the form
  
    {ns1.update/kw1 update-dsl1}
     ns2.update/kw2 update-dsl2
     ...}
  "
  [current updates]
  (reduce-kv
   (fn [c kw update-dsl]
     (if (update? kw)
       (apply-update c kw update-dsl)
       c))
   current
   updates))

(defn dissoc-nonupdates
  [md]
  (reduce
   (fn [acc [k v]]
     (if (and (qualified-keyword? k)
              (clojure.string/index-of (namespace k) ".update"))
       (assoc acc k v)
       acc))
   {}
   md))

(defmethod update-metadata-processor ::cs/file-metadata-update
  [es-url index-name update-event]
  (info "Update: " update-event)
  (es/apply-func index-name doc-type es-url
                 (::md/id update-event)
                 #(apply-updates %
                                 (dissoc-nonupdates update-event))))

(defn response-event
  [r]
  nil)

(defn connection->es-url
  [connection]
  (str (:protocol connection) "://" (:host connection) ":" (:port connection 9200)))

(defrecord MetadataCreate
    [communications started protocol host port profile
     es-url profile-index-name]
  component/Lifecycle
  (start [component]
    (if-not started
      (let [es-url (str protocol "://" host ":" port)
            profile-index (str profile "-" base-index-name)]
        (c/attach-event-handler! communications
                                 :kixi.search/metadata-update
                                 :kixi.datastore.file-metadata/updated
                                 "1.0.0"
                                 (comp response-event
                                       (partial update-metadata-processor es-url profile-index)
                                       :kixi.comms.event/payload))
        (c/attach-validating-event-handler! communications
                                            :kixi.search/sharing-update
                                            :kixi.datastore/sharing-changed
                                            "1.0.0"
                                            (comp response-event
                                                  (partial sharing-change-processor es-url profile-index)))
        (assoc component
               :started true
               :es-url es-url
               :profile-index-name profile-index))
      component))
  (stop [component]))
