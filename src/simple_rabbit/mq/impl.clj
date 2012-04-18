(ns simple-rabbit.impl
  (:use [clojure.tools.logging])
  (:import [java.util.concurrent LinkedBlockingQueue]
           [com.rabbitmq.client
            Connection BasicProperties Channel Envelope AMQP ConnectionFactory Consumer QueueingConsumer]))

;; This forms a thin layer over the existing AMQP libraries and
;; doesn't attempt to do much more than provide a simple clojure layer over
;; the existing api's.

(defn connect
  "Connects to RabbitMQ"
  [host port username password virtual-host]
  (let [#^ConnectionFactory f (doto (ConnectionFactory.)
                                   (.setHost host)
                                   (.setPort port)
                                   (.setUsername username)
                                   (.setPassword password)
                                   (.setVirtualHost virtual-host)
                                   (.setRequestedHeartbeat 0))]
    (.newConnection f)))

(defn disconnect
  [rabbit]
  (.close (:connection rabbit)))

(defn exchange-types
  {:direct "direct"
   :fanout "fanout"
   :topic "topic"
   :federation "x-federation"
   :headers "headers"})

(defn declare-exchange
  "Declares an exchange, with properties being a map of additional string based properties"
  [channel name type durable auto-delete internal properties]
  (info "Declaring exchange:" name ", type:" type ", durable:" durable ", AD:" auto-delete)
  (.exchangeDeclare channel name type durable auto-delete internal properties))

(defn declare-queue
  "Declares a queue, with properties as a map of additional arguments"
  [channel name durable exclusive auto-delete properties]
  (info "Declaring queue:" name ", durable:" durable ", exclusive:" exclusive ", AD:" auto-delete)
  (.queueDeclare channel name durable exclusive auto-delete properties))

(defn bind-queue
  "Binds a queue to an exchange, with routing key and additional properties"
  [channel queue exchange routing-key properties]
  (info "Binding queue:" queue "to exchange:" exchange "on key:" routing-key)
  (.queueBind queue exchange routing-key properties))

(defn unbind-queue
  "Unbinds a queue from an exchange"
  [channel queue exchange routing-key]
  (info "Unbinding queue:" queue "to exchange:" exchange "on key:" routing-key)
  (.queueUnbind queue exchange routing-key))

(defn create-consumer
  "Sets up a consumer for handling messages delivered to a queue"
  [channel f]
  (proxy [com.rabbitmq.client.DefaultConsumer] [channel]
    (handleDelivery [#^String consumerTag #^Envelope envelope
                     #BasicProperties properties body]
      (f (String. body) properties envelope))))

(defn start-consumer
  "Start a consumer in a given channel for a given queue"
  [channel queue consumer autoack]
  (info "Consuming on" queue)
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