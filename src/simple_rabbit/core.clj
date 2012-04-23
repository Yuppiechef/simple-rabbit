(ns simple-rabbit.core
  (:use [clojure.tools.logging])
  (:require [clj-plaza-rabbit.core :as rbt])
  (:require [cheshire.core :as json])
  (:require [clojure.java.io :as io])
  (:import [com.rabbitmq.client.AMQP$BasicProperties$Builder])
  (:import [java.io ByteArrayOutputStream InputStream]))


(defonce ^:dynamic *rabbit* (ref {}))
(def ^:dynamic reply)
(def ^:dynamic route)

(defonce consumers (ref {}))

(defn parsemessage [message content-type]
  (cond
   (.equals "application/json" content-type) (json/parse-string message true)
   :else message)
  )

(defn encodemessage [message content-type]
  (cond
   (.equals "application/json" content-type) (.getBytes (json/encode message))
   (instance? String message) (.getBytes message)
   (instance? InputStream message) (let [out (ByteArrayOutputStream.)]
                                     (io/copy message out) (.toByteArray out))
   :else message))

(defn publish-raw
  "Publish a message through a channel"
  ([rabbit channel exchange routing-key message]
     (publish-raw rabbit channel exchange routing-key com.rabbitmq.client.MessageProperties/TEXT_PLAIN message))
  
  ([rabbit channel exchange routing-key properties message]
     (info (str "*** publishing a message through channel " channel " to exchange " exchange " with routing key " routing-key))
     (.basicPublish (get (deref (:channels rabbit)) channel)
                    exchange
                    routing-key
                    properties
                    message)
     ))

(defn convert-properties [properties]
  (let [map-props (into {} (map #(vector (name (first %)) (second %)) properties))]
    (-> (com.rabbitmq.client.AMQP$BasicProperties$Builder.)
        (.replyTo (:reply-to properties))
        (.contentType (:content-type properties))
        (.headers map-props)
        (.build))))

(defn send-msg
  "Reply to a message based on original message :replyto field."
  ([channel-key exchange routing-key message]
     (send-msg channel-key exchange routing-key message {}))
  
  ([channel-key exchange routing-key message properties]
     (let [content-type (get properties :content-type "application/json")
           props (convert-properties (assoc properties :content-type content-type))
           encoded (encodemessage message content-type)]
       (rbt/get-channel @*rabbit* channel-key)
       (publish-raw @*rabbit* channel-key exchange routing-key props encoded))))

(defn route-msg
  "Send a message to another exchange with the reply key already set"
  ([channel-key replyto exchange routing-key message]
     (route-msg channel-key replyto exchange routing-key message {}))
  
  ([channel-key replyto exchange routing-key message properties]
     (send-msg channel-key exchange routing-key message (assoc properties :reply-to replyto))))

(defn messagefn [consumer-name chankey message properties envelope]
  (let [parsed (parsemessage message (.getContentType properties))
        {:keys [f]} (get @consumers consumer-name)]
    (binding [reply (partial send-msg chankey "" (.getReplyTo properties))
              route (partial route-msg chankey (.getReplyTo properties))]
      (f parsed)))
  )

(defn simple-consumer [consumer-name {:keys [qname chname chankey exchange routing-key]}]
  (rbt/get-channel @*rabbit* chname)
  (try
    (rbt/declare-topic-exchange @*rabbit* chname exchange)
    (catch Exception e (info "Could not declare exchange" exchange ", does it already exist?")))

  (try
    (rbt/make-queue @*rabbit* chname qname exchange routing-key :autodelete false)
    (catch Exception e (info "Could not declare queue" qname ", does it already exist?")))
  (rbt/make-consumer @*rabbit* chname qname #(messagefn consumer-name chankey %1 %2 %3))
    )

(defn register-consumer [nspace qname exchange routing-key f]
  (dosync
   (let [consumer-name (str (.name nspace) "." qname)
         chname (keyword qname)
         chankey (keyword exchange)]
     (alter consumers assoc consumer-name
            {:qname qname
             :chankey chankey
             :chname chname
             :exchange exchange
             :routing-key routing-key
             :f f}))))

(defn connect [old-rabbit & args]
  (apply rbt/connect args))

(defn start-consumers [& connect-args]
  (dosync (apply alter *rabbit* connect connect-args))
  (doseq [[consumer-name consumer] @consumers]
    (if (not (contains? consumer :started))
      (dosync
       (simple-consumer consumer-name consumer)
       (alter consumers assoc consumer-name
              (assoc consumer :started true))
       ))))

(defmacro on
  "Defines a simple message consumer, assumes messages are json encoded and passes the keyword decoded message object as params. Make sure params only has a single par
ameter."
  [queue params & body]
  (let [qname (name queue)
        fnname (symbol (.replaceAll qname "[.]" "-"))]
    
    `(do
       (defn ~fnname [~@params] ~@body)
       (register-consumer *ns* ~(name queue) "process" ~qname ~fnname)
       )
    ))


