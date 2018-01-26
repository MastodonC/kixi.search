(defproject kixi.search "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[aero "1.0.0"]
                 [aleph "0.4.2-alpha8"]
                 [bidi "2.0.12"]
                 [clj-http "3.5.0"]
                 [clojurewerkz/elastisch "3.0.0"]
                 [cider/cider-nrepl "0.15.1"]
                 [cheshire "5.6.3"]
                 [clj-http "3.7.0"]
                 [com.amazonaws/aws-java-sdk "1.11.53" :exclusions [joda-time]]
                 [com.gfredericks/test.chuck "0.2.8"]
                 [com.taoensso/timbre "4.8.0"]
                 [com.fzakaria/slf4j-timbre "0.3.7"]
                 [org.slf4j/log4j-over-slf4j "1.7.25"]
                 [com.walmartlabs/lacinia "0.23.0"]
                 [honeysql "0.9.1"]
                 [kixi/kixi.comms "0.2.31"]
                 [kixi/kixi.log "0.1.4"]
                 [kixi/kixi.metrics "0.4.0"]
                 [kixi/kixi.spec "0.1.13"]
                 [joplin.core "0.3.9"]
                 [joplin.elasticsearch "0.3.9"]
                 [medley "1.0.0"]
                 [org.clojure/clojure "1.9.0-alpha17"]
                 [org.clojure/tools.nrepl "0.2.12"]
                 [com.rpl/specter "1.0.5"]
                 [ring/ring-core "1.6.3"]
                 [ring/ring-jetty-adapter "1.6.3"]
                 [ring/ring-json "0.4.0"]]
  :repl-options {:init-ns user}
  :global-vars {*warn-on-reflection* true
                *assert* false}
  :profiles {:dev {:source-paths ["dev"]}
             :uberjar {:aot [kixi.search.bootstrap]
                       :uberjar-name "kixi.search-standalone.jar"}})
