(ns simple-rabbit.mq.impl
  (:use [clojure.tools.logging])
  (:import [java.util.concurrent LinkedBlockingQueue]
           [com.rabbitmq.client
            Connection Channel Envelope AMQP ConnectionFactory Consumer QueueingConsumer
            RpcClient]))

;; This forms a thin layer over the existing AMQP libraries and
;; doesn't attempt to do much more than provide a simple clojure layer over
;; the existing api's.

(defn connect
  "Connects to RabbitMQ"
  [host port username password virtual-host & [shutdown-fn]]
  (let [port (if (instance? String port) (Integer/parseInt port) port)
        #^ConnectionFactory f (doto (ConnectionFactory.)
                                   (.setHost host)
                                   (.setPort port)
                                   (.setUsername username)
                                   (.setPassword password)
                                   (.setVirtualHost virtual-host)
                                   (.setRequestedHeartbeat 0))
        connection (.newConnection f)]
    (if shutdown-fn
      (.addShutdownListener
       connection
       (proxy [com.rabbitmq.client.ShutdownListener] []
         (shutdownCompleted [cause]
           (shutdown-fn)))))
    connection))

(defn disconnect
  [rabbit]
  (.close rabbit))

(defn channel
  [connection]
  (.createChannel connection))

(defn close-channel
  [channel]
  (.close channel))

(defn publish-raw
  "Publish a message through a channel"
  [channel exchange routing-key message properties]
  (debug (str "Publishing a message with routing key: " routing-key ", properties: " properties))
  (.basicPublish channel exchange routing-key properties message))

(defn convert-properties [properties]
  (info "converting" properties)
  (let [map-props (into {} (map #(vector (name (first %)) (second %)) properties))]
    (-> (com.rabbitmq.client.AMQP$BasicProperties$Builder.)
        (.replyTo (:reply-to properties))
        (.correlationId (:correlation-id properties))
        (.contentType (:content-type properties))
        (.headers map-props)
        (.deliveryMode (Integer. (get properties :delivery-mode 2)))
        (.build))))


(defn exchange-types
  {:direct "direct"
   :fanout "fanout"
   :topic "topic"
   :federation "x-federation"
   :headers "headers"})

(defn declare-exchange
  "Declares an exchange, with properties being a map of additional string based properties"
  [channel nm type durable auto-delete internal properties]
  ;(debug "Declaring exchange:" nm ", type:" type ", durable:" durable ", AD:" auto-delete)
  (.exchangeDeclare channel nm (name type) durable auto-delete properties))


(defn declare-queue
  "Declares a queue, with properties as a map of additional arguments"
  [channel name durable exclusive auto-delete properties]
  ;(debug "Declaring queue:" name ", durable:" durable ", exclusive:" exclusive ", AD:" auto-delete ", Properties:" properties)
  (.queueDeclare channel name
                 durable
                 exclusive
                 auto-delete properties))

(defn bind-queue
  "Binds a queue to an exchange, with routing key and additional properties"
  [channel queue exchange routing-key properties]
  ;(debug "Binding queue:" queue "to exchange:" exchange "on key:" routing-key)
  (.queueBind channel queue exchange routing-key properties))

(defn unbind-queue
  "Unbinds a queue from an exchange"
  [channel queue exchange routing-key]
  ;(debug "Unbinding queue:" queue "to exchange:" exchange "on key:" routing-key)
  (.queueUnbind channel queue exchange routing-key))

(defn bind-exchange
  "Binds an exchange to an exchange, with routing key and additional properties"
  [channel destination source routing-key properties]
  ;(debug "Binding exchange:" source "to exchange:" destination "on key:" routing-key)
  (.exchangeBind channel destination source routing-key properties))

(defn unbind-exchange
  "Unbinds an axchange from an exchange"
  [channel destination source routing-key]
  ;(debug "Unbinding exchange:" source "to exchange:" destination "on key:" routing-key)
  (.queueUnbind channel destination source routing-key))

(defn create-consumer
  "Sets up a consumer for handling messages delivered to a queue"
  [channel f]
  (proxy [com.rabbitmq.client.DefaultConsumer] [channel]
    (handleDelivery [#^String consumerTag #^Envelope envelope
                     properties body]
      (f channel body properties envelope))))

(defn start-consumer
  "Start a consumer in a given channel for a given queue"
  [channel queue consumer autoack]
  ;(debug "Consuming on" queue ", channel" channel)
  
  (.basicConsume channel queue autoack consumer))

(defn publish
  "Publish a message to an exchange"
  [channel exchange routing-key message properties]
  (.basicPublish channel exchange routing-key properties (.getBytes message)))

(defn ack
  "Acknowledge a message as received"
  [channel delivery-tag multiple]
  (.basicAck channel delivery-tag multiple))

(defn nack
  "Reject a message with option to requeue"
  [channel delivery-tag multiple requeue]
  (.basicNack channel delivery-tag multiple requeue))

(defn simple-consumer [channel consumer-name {:keys [qname autoack f] :as consumer}]
  (start-consumer channel qname (create-consumer channel f) autoack))

(defn rpc-client [channel exchange routing-key timeout]
  (RpcClient. channel exchange routing-key timeout))

(defn rpc-call [client message & [properties]]
  (.primitiveCall client (convert-properties properties) message))
