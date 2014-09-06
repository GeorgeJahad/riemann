(ns riemann.blueflood-test
  (:use riemann.blueflood
        riemann.streams
        [riemann.common :exclude [match]]
        riemann.time.controlled
        riemann.time
        [riemann.test :refer [run-stream run-stream-intervals test-stream 
                              with-test-stream test-stream-intervals]]
        [riemann.config :refer [apply!]]
        clojure.test)
  (:require [riemann.index :as index]
            [riemann.folds :as folds]
            [riemann.logging :as logging]
            [cheshire.core :as json]
            [clj-http.client :as client]))

(logging/init)

(use-fixtures :once control-time!)
(use-fixtures :each reset-time!)

(def query-url-template "http://localhost:20000/v2.0/tenant-id/views/%s.%s?from=000000000&to=1500000000&resolution=FULL")

(deftest blueflood-ingest-test
  (let [service (str (java.util.UUID/randomUUID))
        host "a"
        query-url (format query-url-template host service) 
        input
        [{:host host
          :service service
          :metric 2
          :time 2}
         {:host host
          :service service
          :metric 1
          :time 100}]
        stream (blueflood-ingest {:async-queue-name :testq} prn)]
    (apply!)
    (run-stream (sdo stream) input)
    (Thread/sleep 300)
    (is (= [{"numPoints" 1, "timestamp" 2, "average" 2}]
           ((json/parse-string (:body (client/get query-url))) "values")))))
