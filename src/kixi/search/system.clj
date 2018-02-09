(ns kixi.search.system
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [com.stuartsierra.component :as component]
            [kixi.comms :as comms]
            [kixi.comms.components.coreasync :as coreasync]
            [kixi.comms.components.kinesis :as kinesis]
            [kixi.log :as kixi-log]
            [kixi.search.elasticsearch.index-manager :as index-manager]
            [kixi.search.metadata.event-handlers.update :as metadata-update]
            [kixi.search.metadata.query :as metadata-query]
            [kixi.search.repl :as repl]
            [kixi.search.web :as web]
            [medley :as med]
            [taoensso.timbre :as log]))

(defn config
  "Read EDN config, with the given profile. See Aero docs at
  https://github.com/juxt/aero for details."
  [profile]
  (aero/read-config (io/resource "config.edn") {:profile profile}))

(def component-dependencies
  {:communications []
   :repl []
   :metadata-query []
   :metadata-update [:communications]
   :web [:metadata-query]})

(defn new-system-map
  [config]
  (component/system-map
   :communications (case (first (keys (:communications config)))
                     :kinesis (kinesis/map->Kinesis {})
                     :coreasync (coreasync/map->CoreAsync {}))

   :index-manager (index-manager/map->IndexManager {})

   :metadata-query (metadata-query/map->ElasticSearch {})
   :metadata-update (metadata-update/map->MetadataCreate {})

   :repl (repl/map->ReplServer {})
   :web (web/map->Web {})))

(defn raise-first
  "Updates the keys value in map to that keys current first value"
  [m k]
  (assoc m k
         (first (vals (k m)))))

(defn configure-components
  "Merge configuration to its corresponding component (prior to the
  system starting). This is a pattern described in
  https://juxt.pro/blog/posts/aero.html"
  [system config profile]
  (merge-with merge
              system
              (->> (-> config
                       (raise-first :communications))
                   (med/map-vals #(if (map? %)
                                    (assoc % :profile (name profile))
                                    %)))))

(defn configure-logging
  [config]
  (let [level-config {:level (get-in config [:logging :level])
                      :ns-blacklist (get-in config [:logging :ns-blacklist])
                      :timestamp-opts kixi-log/default-timestamp-opts ; iso8601 timestamps
                      :appenders (case (get-in config [:logging :appender])
                                   :println {:println (log/println-appender)}
                                   :json {:direct-json (kixi-log/timbre-appender-logstash)})}]
    (log/set-config! level-config)
    (log/handle-uncaught-jvm-exceptions!
     (fn [throwable ^Thread thread]
       (log/error throwable (str "Unhandled exception on " (.getName thread)))))
    (when (get-in config [:logging :kixi-comms-verbose-logging])
      (log/info "Switching on Kixi Comms verbose logging...")
      (comms/set-verbose-logging! true))))

(defn new-system
  [profile]
  (let [config (config profile)]
    (configure-logging config)
    (-> (new-system-map config)
        (configure-components config profile)
        (component/system-using component-dependencies))))
