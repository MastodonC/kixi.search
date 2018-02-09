(ns kixi.search.elasticsearch.index-manager
  (:require [com.stuartsierra.component :as component]
            [joplin.elasticsearch6.database :as database]
            [joplin.repl :as jrepl]
            [taoensso.timbre :as timbre :refer [info]]))

(defn migrate
  [env migration-conf]
  (->>
   (with-out-str
     (jrepl/migrate migration-conf env))
   (clojure.string/split-lines)
   (run! #(info "JOPLIN:" %))))

(defrecord IndexManager
    [started host port protocol profile]
  component/Lifecycle
  (start [component]
    (if-not started
      (let [joplin-conf {:migrators {:migrator "joplin/kixi/search/elasticsearch/migrators/"}
                         :databases {:es6 {:type :es6
                                           :protocol protocol
                                           :host host
                                           :port port
                                           :profile profile
                                           :migration-index (str profile "-kixi_search-migrations")}}
                         :environments {:env [{:db :es6
                                               :migrator :migrator}]}}]
        (info "Starting Elastic Search Index Manager")
        (migrate :env joplin-conf)
        (assoc component :started true))
      component))
  (stop [component]
    (if started
      (do (info "Stopping Elastic Search Index Manager")
          (dissoc component
                  :started))
      component)))
