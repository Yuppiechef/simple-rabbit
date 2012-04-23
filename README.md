simple-rabbit
=============

simple-rabbit is a wrapper over the Rabbit AMQP library as well as a provider for managing the flow of message logic from queue to queue.

Usage
-----

With the flow logic in mind, we will make an assumption that all local response queues are bound to the default (empty) exchange and will provide their address via the reply_to field in messages.

To keep things simple, we also declare a single direct exchange for services to bind all the service queues to. Intuitively called "services". This is because the flow logic supersedes the flexibility that manually declaring exchanges gives us and then allows us to simplify finding the 'next queue' to just its routing key.

Any request should be placed on the "request" exchange with an appropriate routing key as if going directly to the service in question. This will be pulled by a local rules engine and managed from there.

We define all our rabbit rules in a single (rules) construct which can then be used to create exchanges, queues and their bindings, for example:

   (rules
      (queue auth.simple :durable true :auto-delete false :exclusive false :message-ttl 1000 :arg1 "value1")
      (queue message.encrypt :auto-delete false :arg1 "value1")
      (queue images.cachesize)
      (queue logging.debug)

      ;; More to come on defining flows between multiple queues.
      )


We can define consumers quite simply:

   (on auth.simple [msg]
       (print "Do something with with" msg)
       {:msgtext "Hello World"}
       ;; The function response will become the response to the reply_to queue
       ;; correlation_id will automatically be preserved.
       )

msg will be the actual message passed through, which you can inspect and do stuff with 

To actually start up (or have newly registered consumers fire up), you need to make a call to (start-consumers).

License
-------

Copyright (C) 2012 Yuppiechef.com

Distributed under the Eclipse Public License, the same as Clojure.
