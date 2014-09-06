(ns riemann.blueflood-test
  (:use riemann.blueflood
        riemann.streams
        riemann.time.controlled
        riemann.time
        [riemann.test :refer [run-stream]]
        [riemann.config :refer [apply!]]
        clojure.test)
  (:require 
            [riemann.logging :as logging]
            [cheshire.core :as json]
            [clj-http.client :as client]))

(logging/init)

(use-fixtures :once control-time!)
(use-fixtures :each reset-time!)

; These tests assume you've got blueflood running on the localhost
(def query-url-template
  "http://localhost:20000/v2.0/tenant-id/views/%s.%s?from=000000000&to=1500000000&resolution=FULL")

(defn test-helper [opts]
  (let [service (str (java.util.UUID/randomUUID))
        host "a"
        query-url (format query-url-template host service) 
        timestamp 3
        value 3
        input
        [{:host host
          :service service
          :metric value
          :time timestamp}
         ;; This second event doesn't get included in the batch but the timestamp causes the
         ;;  the batch to complete and be sent to blueflood with just the first event.
         {:time (inc timestamp)}]
        stream (blueflood-ingest opts prn)]
    ;; creates the async executor
    (apply!)
    ;; Feed the input into BF
    (run-stream (sdo stream) input)
    (Thread/sleep 300)
    ;; Read it back from BF
    (is (= ((json/parse-string (:body (client/get query-url))) "values")
           [{"numPoints" 1, "timestamp" timestamp, "average" value}]))))

(deftest blueflood-ingest-test
  ;; test synchronously
  (test-helper {})
  ;; test asynchronously
  (test-helper {:async-queue-name :testq}))


