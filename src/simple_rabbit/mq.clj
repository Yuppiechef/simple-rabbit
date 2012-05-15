(ns simple-rabbit.mq
  (:use [clojure.tools.logging])
  (:require [simple-rabbit.mq.impl :as impl])
  (:require [cheshire.core :as json])
  (:require [clojure.java.io :as io])
  (:import [java.io ByteArrayOutputStream InputStream])
  )

(def ^:dynamic reply)
(def ^:dynamic route)

(defn connect [{:keys [host port username password virtual-host]}]
  (impl/connect host port username password virtual-host))

(defn disconnect [connection]
  (impl/disconnect connection))

(defn channel [connection]
  (impl/channel connection))

(defn close-channel [channel]
  (impl/close-channel channel))

(defonce consumers (ref {}))

(defn parsemessage [message content-type]
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



(defmacro queue [qname & [{:keys [auto-delete exclusive durable msg-ttl]}]]
  `{:name ~(str qname)
    :auto-delete ~(if (nil? auto-delete) false auto-delete)
    :exclusive ~(if (nil? exclusive) false exclusive)
    :durable ~(if (nil? durable) false durable)
    :msg-ttl ~msg-ttl})

(defn rules [& queues] queues)

(defn setup-mq
  "Setup queues and rules on mq"
  [channel rules]
  (doseq [{:keys [name auto-delete exclusive durable msg-ttl] :as queue} rules]
    (impl/declare-queue channel name durable exclusive auto-delete
                        (if msg-ttl {"x-message-ttl" msg-ttl} {}))
    (impl/bind-queue channel name "process" name {})))

(defn send-msg
  [channel exchange routing-key message & [properties]]
  (let [content-type (get properties :content-type "application/json")
        props (impl/convert-properties (assoc properties :content-type content-type))
        encoded (encodemessage message content-type)]
    (if (or (and (not (nil? exchange)) (> (.length exchange) 0))
            (and (not (nil? routing-key)) (> (.length routing-key) 0)))
      (impl/publish-raw channel exchange routing-key encoded props))))

(defn reply-msg [channel reply-id correlation-id message & [properties]]
  (let [props (assoc properties :correlation-id correlation-id)]
    (info "Sending" message "to" reply-id "with properties" (impl/convert-properties props))
    
    (send-msg channel "" reply-id message props))
  )

(defn messagefn
  [f qname channel message properties envelope]
  (try
    (let [parsed (parsemessage message (.getContentType properties))]
      (binding [reply (partial reply-msg channel (.getReplyTo properties) (.getCorrelationId properties))]
        (f parsed properties envelope)))
    (catch Exception e
      (warn "Exception processing message for queue:" qname ", message:" message)
      (.printStackTrace e))))

(defn register-consumer [nspace qname f]
  (let [consumer-name (str (.name nspace) "." qname)]
    (dosync
     (alter consumers assoc consumer-name
            {:qname qname :f #(messagefn f qname %1 %2 %3 %4) :autoack true}))))

(defn start-consumers [connection]
  (doseq [[consumer-name consumer] @consumers]
    (if (not (contains? consumer :started))
      (impl/simple-consumer (channel connection) consumer-name consumer)
      (dosync
       (alter consumers assoc consumer-name
              (assoc consumer :started true))))))

(defn rpc-message [channel exchange routing-key timeout f message & [properties]]
  (with-open [rpc (impl/rpc-client channel exchange routing-key timeout)]
    (let [content-type (get properties :content-type "application/json")
          props (assoc properties :content-type content-type)
          encoded (encodemessage message content-type)
          result (impl/rpc-call rpc encoded props)
          parsed (parsemessage (String. result "UTF-8") (get props :response-type "application/json"))]
      (f parsed)
      )))



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