simple-rabbit
=============

simple-rabbit is a RabbitMQ library that wraps the AMQP client library, provides connection state management and abstracted functions to quickly be able to register consumers, send messages and do rpc calls via RabbitMQ.

It also handles wrapping clojure maps to and from JSON, using the content_type application/json as a marker for that.

When the connection to RabbitMQ is lost, it will automatically try to open the connection again every second - If connection is re-opened, it will re-register all the consumers.

Usage
-----

Assuming you're using leiningen to manage your dependencies, add this library to your project.clj:

```clojure
  [com.yuppiechef/simple-rabbit "1.0.0"]
```

Now, lets build a simple echo service example - slightly more verbose code for this is in the src/simple-rabbit/example.clj

First, lets get the namespace setup, for brevity I'll 'use' the simple-rabbit.state namespace instead of 'require':

```clojure
(ns simple-rabbit.example
  (:use [simple-rabbit.state]))
```

By default, the connection to RabbitMQ is to localhost, port 5672, virtualhost /, username guest and password guest, but you can set this by calling set-config before you call setup-rules :

```clojure
(set-config {:host "localhost" :virtual-host "/" :port "5672" :username "guest" :password "guest"})
```

Lets define a few basic echo function :

```clojure
(defn echo [msg props envelope]
  (reply props msg))
```

echo will be our echo service and it will reply with the message as-is.

Then, let's define our queue, exchange and binding rules:

```clojure
(def mqrules
  (rules
   (exchange process :fanout)
   (exchange publish :topic)
   (bind :exchange publish process)
   
   (queue test.echo :msg-fn #'echo)))
```

Above, we're defining exchanges called "process" and "publish" - by default queues bind to "publish" exchange and messages send to the "process" exchange, so we'll just bind messages to go from one to the other.

After we defined our exchanges, we'll define our echo queue and tell it that we want a consumer on that queue which will call our echo function.

If you evaluate this, you will find that mqrules ends up just being a vector of hash-maps. Nothing has started yet. Note that this means you can conj a bunch of rules together and start them all up at once. Alternatively, you can just run (setup-rules) on each set of rules.

To fire everything off, call:

```clojure
(setup-rules mqrules)
```

If you look at your RabbitMQ instance, you should now see the "process" and "publish" exchanges, bound to each other and a "test.echo" queue bound to the "process" exchange with the key "test.echo".

Let's now do an rpc call to make sure the consumer works:

```clojure
(rpc "test.echo" {:msg "rpc!"} #(println "RPC Result:" %1) 3000 #(println "RPC Timeout"))
```

You should see 'RPC Result {:msg "rpc!"}' printed out on your repl. If the bindings weren't correct or something broke, it would say 'RPC Timeout' after 3 seconds.

You can override the default publishing/consuming exchange topology by simply specifying it.


Look at example.clj for more!

License
-------

Copyright (C) 2012 Yuppiechef.com

Distributed under the Eclipse Public License, the same as Clojure.
