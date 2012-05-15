(ns simple-rabbit.state
  (:use [clojure.tools.logging])
  (:require [simple-rabbit.mq :as mq]))



(def rabbit-config (atom {:host "localhost" :virtual-host "/" :port "5672" :username "guest" :password "guest"}))
(def rabbit (atom nil))

(defn set-config [{:keys [host virtual-host port username password] :as config}]
  (compare-and-set! rabbit-config @rabbit-config config))

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
    (mq/send-msg chan "publish" routing-key message properties)))

(defn rpc [routing-key message result-fn timeout]
  (check-connection)
  (.start
   (Thread.
    #(mq/rpc-message (mq/channel @rabbit) "publish" routing-key timeout result-fn message))))

(defn setup-rules [rules]
  (with-open [chan (channel)]
    (mq/setup-mq chan rules)))

(defn start-consumers []
  (check-connection)
  (mq/start-consumers @rabbit))

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
       (mq/register-consumer *ns* ~qname ~fnname))
    ))
