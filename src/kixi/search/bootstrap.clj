(ns kixi.search.bootstrap
  (:require [kixi.search.system :as system]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :as log])
  (:gen-class))

(defn -main
  [& args]
  (let [config-profile (keyword (first args))
        system (system/new-system config-profile)]
    (.addShutdownHook
     (Runtime/getRuntime)
     (Thread. #(component/stop-system system)))
    (try
      (component/start-system system)
      (.. (Thread/currentThread) join)
      (catch Throwable t
        (log/error t "Top level exception caught")))))
