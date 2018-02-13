(ns kixi.spec.test-helper
  (:require [clojure data
             [test :refer :all]]
            [clojure.spec.alpha :as s]
            [clojure.test :refer :all]
            [environ.core :refer [env]]
            [clojure.spec.test.alpha :as stest]
            [clojure.test.check :as tc]
            [taoensso.timbre :as timbre :refer [error]]
            [com.gfredericks.test.chuck.clojure-test :as chuck]))

(def sample-size 20)

(defn check
  [sym]
  (-> sym
      (stest/check {:clojure.spec.test.check/opts {:num-tests sample-size}
                    :max-tries 100})
      first
      stest/abbrev-result
      :failure))

(defmacro is-check
  "Dirty macro that wraps spec.check and tests for no failures. Should upgrade to use do-report"
  [sym]
  `(is (nil? (check ~sym))))

(defmacro checking
  "Wraps bodys for test.chucks checking in a try catch that converts spec problems, instrumentation failures say, into clojure.test reports"
  [name bindings & body]
  `(chuck/checking ~name
                   ~sample-size
                   ~bindings
                   (try
                     ~@body
                     (catch clojure.lang.ExceptionInfo t#
                       (let [data# (ex-data t#)]
                         (if (::s/problems data#)
                           (doseq [problem# (::s/problems data#)]
                             (clojure.test/do-report {:type :fail
                                                      :actual problem#}))
                           (throw t#))))
                     (catch Exception e#
                       (clojure.test/do-report {:type :fail
                                                :actual e#})))))

(defmacro is-submap
  [expected actual & [msg]]
  `(try
     (let [act# ~actual
           exp# ~expected
           [only-in-ex# only-in-ac# shared#] (clojure.data/diff exp# act#)]
       (if only-in-ex#
         (clojure.test/do-report {:type :fail
                                  :message (or ~msg "Missing expected elements.")
                                  :expected only-in-ex# :actual act#})
         (clojure.test/do-report {:type :pass
                                  :message "Matched"
                                  :expected exp# :actual act#})))
     (catch Throwable t#
       (clojure.test/do-report {:type :error :message "Exception diffing"
                                :expected nil :actual t#}))))

(defmacro is-match
  [expected actual & [msg]]
  `(try
     (let [act# ~actual
           exp# ~expected
           [only-in-ac# only-in-ex# shared#] (clojure.data/diff act# exp#)]
       (cond
         only-in-ex#
         (clojure.test/do-report {:type :fail
                                  :message (or ~msg "Missing expected elements.")
                                  :expected only-in-ex# :actual act#})
         only-in-ac#
         (clojure.test/do-report {:type :fail
                                  :message (or ~msg "Has extra elements.")
                                  :expected {} :actual only-in-ac#})
         :else (clojure.test/do-report {:type :pass
                                        :message "Matched"
                                        :expected exp# :actual act#})))
     (catch Throwable t#
       (clojure.test/do-report {:type :error :message "Exception diffing"
                                :expected ~expected :actual t#})))  )

(def wait-tries (Integer/parseInt (env :wait-tries "200")))
(def wait-per-try (Integer/parseInt (env :wait-per-try "20")))

(defn wait-for-pred
  ([p]
   (wait-for-pred p wait-tries))
  ([p tries]
   (wait-for-pred p tries wait-per-try))
  ([p tries ms]
   (loop [try tries
          pre-result nil]
     (if (pos? try)
       (let [result (p)]
         (if-not (:pass result)
           (do
             (Thread/sleep ms)
             (recur (dec try) result))
           result))
       pre-result))))

(defn ensure-pred
  ([p]
   (ensure-pred p wait-tries))
  ([p tries]
   (ensure-pred p tries wait-per-try))
  ([p tries ms]
   (loop [try tries
          pre-result nil]
     (if (pos? try)
       (let [result (p)]
         (do
           (Thread/sleep ms)
           (recur (dec try) result)))
       pre-result))))

(defmacro wait-is=
  [expected actual & [msg]]
  `(try
     (let [act# ~actual
           exp# ~expected
           result# (wait-for-pred #(let [r# ~actual]
                                     (if (= exp#
                                            r#)
                                       {:pass true
                                        :result r#}
                                       {:pass false
                                        :result r#})))]
       (if (:pass result#)
         (clojure.test/do-report {:type :pass
                                  :message "Matched"
                                  :expected exp# :actual (:result result#)})
         (clojure.test/do-report {:type :fail
                                  :message (or ~msg "Out come never acheived")
                                  :expected exp# :actual (:result result#)})))
     (catch Throwable t#
       (clojure.test/do-report {:type :error :message "Exception waiting"
                                :expected ~expected :actual t#}))))

(defmacro always-is=
  [expected actual & [msg]]
  `(try
     (let [act# ~actual
           exp# ~expected
           result# (ensure-pred #(let [r# ~actual]
                                   (if (= exp#
                                          r#)
                                     {:pass true
                                      :result r#}
                                     {:pass false
                                      :result r#})))]
       (if (:pass result#)
         (clojure.test/do-report {:type :pass
                                  :message "Matched"
                                  :expected exp# :actual (:result result#)})
         (clojure.test/do-report {:type :fail
                                  :message (or ~msg "Out come never acheived")
                                  :expected exp# :actual (:result result#)})))
     (catch Throwable t#
       (clojure.test/do-report {:type :error :message "Exception waiting"
                                :expected ~expected :actual t#}))))
