(ns riemann.blueflood
  "Forwards events to Blueflood"
  (:require [clj-http.client :as client]
            [cheshire.core :as json]
            [clojure.string :as s]
            [clojure.tools.logging :as logging]))

(def version "0.1")
(defn prep-event-for-bf [ev]
  {:collectionTime (:time ev)
   :ttlInSeconds (if-let [rttl (:ttl ev)]
                   rttl 2592000)
   :metricValue (:metric ev)
   :metricName (s/join "." [(:host ev) (:service ev)])})

(defn bf-body [evs]
  (->> evs
       (map prep-event-for-bf)
       json/generate-string))

(defn log-bf-body [evs]
  (let [r (bf-body evs)]
    (logging/info "bf-body" r)
    r))



(defn bf-ingest [evs]
  ; TODO: future performance enhancements: switch to async client, use keep http connection open
  (logging/info version
    {:method :post
     :url "http://192.168.5.3:19000/v1.0/tenant-id/experimental/metrics"
     :content-type :json
     :body (bf-body evs)}))


(defn log-bf-ingest [evs]
  (logging/info "ingest_output" (bf-ingest evs)))
