(ns kixi.search.system
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [com.stuartsierra.component
             :as
             component]
            [kixi
             [comms :as comms]
             [log :as kixi-log]]
            [kixi.search.web :as w]
            [kixi.search.repl :as repl]
            [kixi.search.elasticsearch.query :as query]
            [kixi.comms.components
             [kinesis :as kinesis]
             [coreasync :as coreasync]]
            [taoensso.timbre :as log]))


(defn config
  "Read EDN config, with the given profile. See Aero docs at
  https://github.com/juxt/aero for details."
  [profile]
  (aero/read-config (io/resource "config.edn") {:profile profile}))

(def component-dependencies
  {:communications []
   :repl []
   :query []
   :web [:query]})

(defn new-system-map
  [config]
  (component/system-map
   :communications (case (first (keys (:communications config)))
                     :kinesis (kinesis/map->Kinesis {})
                     :coreasync (coreasync/map->CoreAsync {}))
   :query (query/map->ElasticSearch {})
   :repl (repl/map->ReplServer {})
   :web (w/map->Web {})))

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
              (-> config
                  (raise-first :communications))))

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
