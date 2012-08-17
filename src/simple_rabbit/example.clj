(ns simple-rabbit.example
  (:use [simple-rabbit.state]))

(defn echo [msg props envelope]
  (reply props msg))

(defn printservice [msg & _]
  (println msg))

(defn send-echo [str]
  (send-msg "test.echo" {:msg str} {:reply-to "test.print"}))

(comment
  (def mqrules
    (rules
     (exchange process :fanout)
     (exchange publish :topic)
     (bind :exchange publish process)
     
     (queue test.echo :msg-fn #'echo :durable false :exchange "testprocess" :routing-key "news.*")
     (queue test.echoagain)
     (queue test.print :msg-fn #'printservice)
     (on-message test.echoagain #'echo)))

  ;; So now we setup rules :
  (setup-rules mqrules)

  ;; Then we can call echo:
  (send-echo "Good morning")

  ;; Let's try an rpc:
  (rpc "test.echo" {:msg "rpc!"} #(println "RPC Result:" %1) 3000 #(println "RPC Timeout"))

  ;; Let's try an rpc timeout:
  (rpc "test.echo_not_there" {:msg "rpc!"} #(println "RPC Result:" %1) 1000 #(println "RPC Timeout")))