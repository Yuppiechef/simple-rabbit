(ns simple-rabbit.mq
  (:require [simple-rabbit.impl :as impl])
  )

;; Provides an abstraction over the simpler singular constructs of the
;; implementation layer.



;; Sample
(rabbit
 (queue "stockorder.update" :durable) ; Default values - durable,
                                        ; non-autodelete,
                                        ; non-exclusive
 
 ;; Define all the exchanges and bindings in one place.
 (exchanges
  (topic "stockorder" :durable
         (bind-queue "stockorder.update" "stockorder.update" )
         (bind-exchange "integrate" "*"))
  (topic "product"
         (bind-queue "product.update")
         (bind-exchange "integrate" "*"))))

;; Define the flow rules
(rules
 (flow decrypt-and-auth [& body]
       (route "message" "message.auth")
       ~@body
       )

 
 (on "product.startupdate"  ;; queue
     (decrypt-and-auth
      (route "product" "product.update")
      (fork "product" "imagecache"))
     )
 (on "product.update.forrealsies" product-update))


(defn product-update
  ;; Do work based on message
  [msg]
  
  
  ;; reply directly to reply_to if possible
  {:arb "data that gets sent to reply_to with correlation_id intact."}
  )

(defn scan-line
  
  )

(defn send-image
  )