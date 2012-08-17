(ns simple-rabbit.state
  (:use [clojure.tools.logging])
  (:require [simple-rabbit.mq :as mq])
  (:require [simple-rabbit.mq.impl :as impl]))

(defonce rabbit-config (atom {:host "localhost" :virtual-host "/" :port "5672" :username "guest" :password "guest"}))
(defonce rabbit (atom nil))
(defonce disconnecting (atom false))
(defonce installed-rules (atom []))
(defonce startup-fn (atom #()))
(defonce message-send-headers (atom {}))

(defn set-config [{:keys [host virtual-host port username password] :as config}]
  (compare-and-set! rabbit-config @rabbit-config config))

;; Default headers to send on every message - hint: authentication token.
(defn set-message-headers! [headermap]
  (compare-and-set! message-send-headers @message-send-headers headermap))

(defn add-message-header! [key val]
  (swap! message-send-headers assoc key val))

(defn handle-shutdown [reconnect-fn]
  (info "Handling Shutdown")
  (mq/stop-consumers)
  (when (not @disconnecting)
    (loop [started false]
      (if started (Thread/sleep 5000))
      (info "Reconnecting...")
      (try
        (reconnect-fn)
        (info "Reconnected - " @rabbit)
        (catch Throwable e (info e)))
      (if (nil? @rabbit) (recur true) (@startup-fn)))
      ))

(defn disconnect []
  (compare-and-set! disconnecting @disconnecting true)
  (try
    (mq/disconnect @rabbit)
    (catch Exception e (info "Could not disconnect old connection.")))
  (compare-and-set! rabbit @rabbit nil)
  (compare-and-set! disconnecting @disconnecting false))

(defn connect []
  (if (not (nil? @rabbit))
    (disconnect))
  (info "Connecting to RabbitMQ.")
  (compare-and-set! rabbit @rabbit (mq/connect
                                    (assoc @rabbit-config :shutdown-fn
                                           #(handle-shutdown #'connect)))))

(defn check-connection []
  (if (nil? @rabbit)
    (connect)))

(defn channel []
  (check-connection)
  (mq/channel @rabbit))

(defn send-msg [routing-key message & [properties exchange]]
  (with-open [chan (channel)]
    (mq/send-msg chan (or exchange "publish") routing-key message (merge @message-send-headers properties))))

(defn reply [original-properties message & [properties]]
  (with-open [chan (channel)]
    (apply mq/reply-msg chan original-properties message properties)))


(defn rpc [routing-key message result-fn timeout timeout-fn & [properties exchange]]
  (check-connection)
  (.start
   (Thread.
    #(with-open [chan (channel)]
       (mq/rpc-message chan
                       (or exchange "publish")
                       routing-key timeout result-fn timeout-fn message
                       (merge @message-send-headers properties))))))

(defn setup-rules [rules]
  (info "Setup rules")
  (check-connection)
  (swap! installed-rules conj rules)
  (mq/setup-mq @rabbit rules))


(defn start-consumers []
  (info "Start consumers")
  (check-connection)
  (mq/start-consumers @rabbit))

(defn- start-mq []
  (let [rules @installed-rules]
      (compare-and-set! installed-rules rules [])
      (doseq [rule rules]
        (setup-rules rule))
      (start-consumers)))

(compare-and-set! startup-fn @startup-fn start-mq)

(defn consume
  "Fires up an anonymous exclusive queue which binds to the process exchange (by default) by routing key and calls listen-fn when a message arrives - You can stop the consumer by calling stop-consumer on the result of this function."
  [routing-key listen-fn & [exchange]]
  (let [chan (channel)
        declareok (impl/declare-queue chan "" false true true {})
        queue-name (.getQueue declareok)]
    
    (impl/bind-queue chan queue-name (or exchange "process") routing-key {})
    (impl/simple-consumer chan "" {:qname queue-name :autoack true :f (partial #'mq/messagefn listen-fn queue-name)})
    chan
    ))

(defn stop-consumer [consumer]
  (.close consumer))


;; Convenience aliases.
(def rules mq/rules)
(defmacro queue [& args] `(mq/queue ~@args))
(defmacro on-message [& args] `(mq/on-message ~@args))
(defmacro exchange [& args] `(mq/exchange ~@args))
(defmacro bind [& args] `(mq/bind ~@args))
