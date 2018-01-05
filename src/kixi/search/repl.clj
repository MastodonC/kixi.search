(ns kixi.search.repl
  (:require [com.stuartsierra.component :as component]
            [cider.nrepl :refer [cider-nrepl-handler]]
            [clojure.tools.nrepl.server :as nrepl-server]
            [taoensso.timbre :as log]))

(defrecord ReplServer
    [port]
  component/Lifecycle
  (start [this]
    (log/info (str "Starting REPL server on " port))
    (assoc this :repl-server
           (nrepl-server/start-server :handler cider-nrepl-handler
                                      :bind "0.0.0.0"
                                      :port port)))
  (stop [this]
    (log/info "Stopping REPL server")
    (if (:repl-server this)
      (do (nrepl-server/stop-server (:repl-server this))
          (dissoc this :repl-server))
      this)))
