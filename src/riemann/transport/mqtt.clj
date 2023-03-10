(ns riemann.transport.mqtt
  "Accepts messages from external sources. Associated with a core. Sends
  incoming events to the core's streams, queries the core's index for states."
  (:require [riemann.query         :as query]
            [riemann.index         :as index]
            [riemann.pubsub        :as p]
            [riemann.test          :as test]
            [interval-metrics.core :as metrics]
            [clojure.java.io :as io]
            [clojurewerkz.machine-head.client :as mh])
  (:use [riemann.common        :only [event-to-json ensure-event-time]]
        [riemann.instrumentation :only [Instrumented]]
        [riemann.service       :only [Service ServiceEquiv]]
        [riemann.time          :only [unix-time]]
        [clojure.tools.logging :only [info warn debug]]
        [riemann.common :only [decode-inputstream encode]]
        [riemann.transport :only [handle]]))

(def ^:no-doc pb->msg
  #(-> % (io/input-stream) (decode-inputstream)))


(defn- message-handler
    [^String topic _ ^bytes payload core stats]
  (try
    (info "mqtt sub" payload)
    (let [msg (pb->msg payload)
          result (handle core msg)]
         (metrics/update! stats (- (System/nanoTime) (:decode-time msg))))
      
    (catch Exception e
      (let [errmsg (.getMessage e)]
        (warn "mqtt caught an exception:" errmsg)))))
    


(defrecord MQTTClient [connectionString topics core stats conn]
  ServiceEquiv
  (equiv? [this other]
          (and (instance? MQTTClient other)
               (= connectionString (:connectionString other))))

  Service
  (conflict? [this other]
             (and (instance? MQTTClient other)
                  (= connectionString (:connectionString other))))

  (reload! [this new-core]
           (reset! core new-core))

  (start! [this]
          (when-not test/*testing*
            (locking this
              (when-not @conn
                (reset! conn (mh/connect connectionString))
                (info "conn" conn)
                (mh/subscribe @conn topics (fn [^String topic _ ^bytes payload] (message-handler topic _ payload core stats)))
                (info "MQTT Client" connectionString "connected")))))

  (stop! [this]
         (locking this
           (when @conn
             (mh/disconnect conn)
             (info "MQTT " connectionString "disconnect"))))

  Instrumented
  (events [this]
    (let [svc (str "riemann transport mqtt " connectionString)
          in (metrics/snapshot! stats)
          base {:state "ok"
                :tags ["riemann"]
                :time (:time in)}]
      (map (partial merge base)
           (concat [{:service (str svc " in rate")
                     :metric (:rate in)}]
                   (map (fn [[q latency]]
                          {:service (str svc " in latency " q)
                           :metric latency})
                        (:latencies in)))))))

(defn mqtt-client
  "Starts a new mqtt client for a core. Starts immediately.

  Options:

  - :host   The address to listen on (default 127.0.0.1)
  - :port   The port to listen on (default 5556)"
  ([] (mqtt-client {}))
  ([opts]
   (MQTTClient.
     (get opts :connectionString "tcp://broker-cn.emqx.io:1883")
     (get opts :topics {"/riemann/topic/#" 0})
     (atom nil)
     (metrics/rate+latency)
     (atom nil))))
    
