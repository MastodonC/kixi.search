(ns kixi.search.time
  (:require [clj-time.core :as t]
            [clj-time.format :as tf]))

(def format :basic-date-time)

(def es-format
  (str (clojure.string/replace (name format) "-" "_") "||" "yyyyMMdd"))
