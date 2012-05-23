(ns simple-rabbit.state
  (:use [clojure.tools.logging])
  (:require [simple-rabbit.mq :as mq])
  (:require [simple-rabbit.mq.impl :as impl]))

(def rabbit-config (atom {:host "localhost" :virtual-host "/" :port "5672" :username "guest" :password "guest"}))
(def rabbit (atom nil))

(defn set-config [{:keys [host virtual-host port username password] :as config}]
  (compare-and-set! rabbit-config @rabbit-config config))

;; Default headers to send on every message - hint: authentication token.
(def message-send-headers (atom {}))
(defn set-message-headers! [headermap]
  (compare-and-set! message-send-headers @message-send-headers headermap))

(defn add-message-header! [key val]
  (swap! message-send-headers assoc key val))

(defn connect []
  (if (not (nil? @rabbit))
    (try
      (mq/disconnect @rabbit)
      (catch Exception e (info "Could not disconnect old connection."))))
  (compare-and-set! rabbit @rabbit (mq/connect @rabbit-config)))

(defn check-connection []
  (if (nil? @rabbit)
    (connect)))

(defn channel []
  (check-connection)
  (mq/channel @rabbit))

(defn send-msg [routing-key message & properties]
  (with-open [chan (channel)]
    (mq/send-msg chan "publish" routing-key message (merge @message-send-headers properties))))

(defn rpc [routing-key message result-fn timeout timeout-fn & properties]
  (check-connection)
  (.start
   (Thread.
    #(mq/rpc-message (mq/channel @rabbit) "publish" routing-key timeout result-fn timeout-fn message
                     (merge @message-send-headers properties)))))

(defn setup-rules [rules]
  (with-open [chan (channel)]
    (mq/setup-mq chan rules)))

(defn start-consumers []
  (check-connection)
  (mq/start-consumers @rabbit))

(defn consume
  "Fires up an anonymous exclusive queue which binds to the process exchange by routing key and calls listen-fn when a message arrives - You can stop the consumer by calling stop-consumer on the result of this function."
  [routing-key listen-fn]
  (let [chan (channel)
        declareok (impl/declare-queue chan "" false true true {})
        queue-name (.getQueue declareok)]
    
    (impl/bind-queue chan queue-name "process" routing-key {})
    (impl/simple-consumer chan "" {:qname queue-name :autoack true :f (partial #'mq/messagefn listen-fn queue-name)})
    chan
    ))

(defn stop-consumer [consumer]
  (.close consumer))


;; Convenience aliases.
(def rules mq/rules)
(defmacro queue [& args] `(mq/queue ~@args))

;; Note, this is a terrible macro need to figure out a more funcitonal
;; approach to declaring consumers.

;; The problem with this macro is that, even though it's easy to use,
;; it's complex in the sense that it defines a function and
;; essentially updates a hidden global variable which then affects
;; what another function (start-consumers) does implicitly, which is
;; not ideal. A better thing for 'on' to do would be to return an
;; hashmap that start-consumers accepts and uses to start all the
;; consumers.

;; That way, at least, what will be worked on is completely transparent.
(defmacro on
  [queue params & body]
  (let [qname (name queue)
        fnname (symbol (.replaceAll qname "[.]" "-"))]
    `(do
       (defn ~fnname [~@params] ~@body)
       (mq/register-consumer *ns* ~qname #(~fnname %1 %2 %3)))
    ))


(defn something []

  (let [consumer (consume "stockorder.updates.123" (fn [msg & props] (print msg)))]
    (stop-consumer consumer)))
