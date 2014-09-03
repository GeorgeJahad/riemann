(ns riemann.blueflood
  "Forwards events to Blueflood"
  (:require [clj-http.client :as client]
            [cheshire.core :as json]
            [clojure.string :as s]
            [clojure.tools.logging :as logging]))

(def version "0.1")
(def url-template "http://%s:%s/v2.0/%s/ingest")
(def defaults
  {:ttl 2592000
   :host "localhost"
   :port "19000"
   :tenant-id "tenant-id"})

(defn prep-event-for-bf [ev]
  {:collectionTime (:time ev)
   :ttlInSeconds (or (:ttl ev) (defaults :ttl))
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



(defn blueflood-ingest [{:keys [host port tenant-id]
                         :as opts
                         :or {host (defaults :host)
                              port (defaults :port)
                              tenant-id (defaults :tenant-id)}}]
  ; TODO: future performance enhancements: switch to async client, use keep http connection open
  (let [url (format url-template host port tenant-id)]
    (fn [evs]
#_      (client/post url
               {:body (bf-body evs)
                :socket-timeout 5000
                :conn-timeout 5000
                :content-type :json
                :accept :json
                :throw-entire-message? true})
      (logging/info version
                    {:method :post
                     :url url
                     :content-type :json
                     :body (bf-body evs)}))))


#_(defn log-bf-ingest [evs]
  (logging/info "ingest_output" (bf-ingest evs)))
