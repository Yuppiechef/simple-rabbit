(ns simple-rabbit.mq
  (:use [clojure.tools.logging])
  (:require [simple-rabbit.mq.impl :as impl])
  (:require [cheshire.core :as json])
  (:require [clojure.java.io :as io])
  (:import [java.io ByteArrayOutputStream InputStream])
  )

(defn connect [{:keys [host port username password virtual-host shutdown-fn]}]
  (impl/connect host port username password virtual-host shutdown-fn))

(defn disconnect [connection]
  (impl/disconnect connection))

(defn channel [connection]
  (impl/channel connection))

(defn close-channel [channel]
  (impl/close-channel channel))

(defonce consumers (atom {}))

(defn parsemessage
  "Parse a message with a given content type."
  [message content-type]
  (cond
   (.equals "application/json" content-type)
   (try (json/parse-string message true)
        (catch Exception e
          (warn "Could not parse json message?" message)
          message))
   :else message)
  )

(defn encodemessage [message content-type]
  (cond
   (.equals "application/json" content-type) (.getBytes (json/encode message))
   (instance? String message) (.getBytes message)
   (instance? InputStream message) (let [out (ByteArrayOutputStream.)]
                                     (io/copy message out) (.toByteArray out))
   :else message))

(defn send-msg
  [channel exchange routing-key message & [properties]]
  (let [content-type (get properties :content-type "application/json")
        props (impl/convert-properties (assoc properties :content-type content-type))
        encoded (encodemessage message content-type)]
    (if (or (and (not (nil? exchange)) (> (.length exchange) 0))
            (and (not (nil? routing-key)) (> (.length routing-key) 0)))
      (impl/publish-raw channel exchange routing-key encoded props))))

(defn reply-msg [channel original-properties message & [properties]]
  (let [props (assoc properties :correlation-id (.getCorrelationId original-properties))]
    (send-msg channel "" (.getReplyTo original-properties) message props)))

(defn messagefn
  [f qname channel message properties envelope]
  (try
    (let [parsed (parsemessage message (.getContentType properties))
          args [:msg parsed :properties properties :envelope envelope :channel channel :qname qname]
          response (apply f args)]
      (cond
       (vector? response) (reply-msg channel properties (first response) (second response))
       response (reply-msg channel properties response)))
    (catch Exception e
      (warn "Exception processing message for queue:" qname ", message:" message)
      (.printStackTrace e))))

(defn consumer-registered? [nspace qname]
  (let [consumer-name (str (.name nspace) "." qname)]
    (contains? @consumers consumer-name)))


(defn register-consumer [nspace qname f]
  (let [consumer-name (str (.name nspace) "." qname)
        consumer (get @consumers consumer-name {})
        newconsumer (merge consumer
                           {:qname qname :f #(messagefn f qname %1 %2 %3 %4) :autoack true})]
    (swap! consumers assoc consumer-name newconsumer)))

(defn start-consumers [connection]
  (doseq [[consumer-name consumer] @consumers]
    (when-not (contains? consumer :started)
      (let [chan (channel connection)]
        (impl/simple-consumer chan consumer-name consumer)
        (swap! consumers assoc consumer-name
               (assoc consumer :started true :channel chan))))))

(defn stop-consumer [consumer-name consumer]
  (when (contains? consumer :started)
    (try 
      (.close (:channel consumer))
      (catch Throwable e))
    (swap! consumers assoc consumer-name
           (dissoc consumer :started :channel)))  )

(defn stop-consumers []
  (doseq [[consumer-name consumer] @consumers]
    (stop-consumer consumer-name consumer)))

(defn rpc-message [channel exchange routing-key timeout f timeout-fn message & [properties]]
  (try
    (with-open [rpc (impl/rpc-client channel exchange routing-key timeout)]
      (let [content-type (get properties :content-type "application/json")
            props (assoc properties :content-type content-type)
            encoded (encodemessage message content-type)
            result (impl/rpc-call rpc encoded props)
            parsed (parsemessage (String. result "UTF-8") (get props :response-type "application/json"))]
        (f parsed)
        ))
    (catch java.util.concurrent.TimeoutException e (timeout-fn))
    (catch Exception e (.printStackTrace e) (timeout-fn))))

(defmacro exchange [name & [exchange-type & {:keys [durable auto-delete internal properties]
                                             :or {durable true, auto-delete false, internal false}}]]
  `{:type :exchange
    :name ~(str name)
    :exchange-type ~(or exchange-type :topic)
    :durable ~(or durable)
    :auto-delete ~auto-delete
    :internal ~internal
    :properties ~properties
    :ns *ns*
    })

(defmacro bind [bind-type source destination & [routing-key properties]]
  `{:type :bind
    :bind-type ~bind-type
    :source ~(str source)
    :destination ~(str destination)
    :routing-key ~(or routing-key "")
    :properties ~properties
    })

(defmacro queue [qname & {:keys [auto-delete exclusive durable msg-ttl msg-fn exchange routing-key]
                          :or {durable true, exclusive false, auto-delete false, exchange "process"}}]
  `{:type :queue
    :exchange ~exchange
    :name ~(str qname)
    :routing-key ~(or routing-key (str qname))
    :auto-delete ~auto-delete
    :exclusive ~exclusive
    :durable ~durable
    :msg-ttl ~msg-ttl
    :msg-fn ~msg-fn
    :ns *ns*})

(defmacro on-message [qname fn]
  `{:type :consumer
    :qname ~(str qname)
    :msg-fn ~fn
    :ns *ns*})

(defn rules [& rules] rules)

(defmulti start-rule (fn [_ rule] (:type rule)))

(defmethod start-rule :consumer [connection {:keys [ns qname msg-fn]}]
  (register-consumer ns qname msg-fn)
  )

(defmethod start-rule :queue [connection {:keys [name auto-delete exclusive durable msg-ttl msg-fn ns exchange routing-key] :as queue}]
  (try
    (with-open [chan (channel connection)]
      (impl/declare-queue chan name durable exclusive auto-delete
                          (if msg-ttl {"x-message-ttl" msg-ttl} {})))
    (catch Exception e (warn "Could not declare queue:" (.getMessage e))))

  (with-open [chan (channel connection)]
    (impl/bind-queue chan name exchange routing-key {}))
  (when msg-fn
    (start-rule connection
                {:type :consumer :qname name :ns ns :msg-fn msg-fn}))
  )

(defmethod start-rule :exchange [connection {:keys [name exchange-type durable auto-delete internal properties]}]
  (try
    (with-open [chan (channel connection)]
      (impl/declare-exchange chan name (get impl/exchange-types exchange-type exchange-type) durable auto-delete internal properties))
    (catch Exception e (warn "Could not declare exchange:" (.getMessage e)))))

(def bind-types
  {:exchange impl/bind-exchange
   :queue impl/bind-queue})

(defmethod start-rule :bind [connection {:keys [bind-type source destination routing-key properties]}]
  (let [bind (get bind-types bind-type (fn [& _] nil))]
    (with-open [chan (channel connection)]
      (bind chan destination source routing-key properties))))

(defn setup-mq
  "Setup queues and rules on mq, start registered consumers"
  [connection rules]
  (doseq [rule rules]
    (start-rule connection rule))
  (start-consumers connection))

(comment
  (rules
   (queue stockorder.update)
   (queue orders.packingslip)
   (queue orders.packingslipresult {:msg-ttl 60000})
   (queue workers.update)
   (queue images.cachesize))

  (on workers.update [msg properties]
      ;;do something
      (reply {:return "something"} {:content-type "application/json"})
      ))