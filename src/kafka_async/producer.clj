(ns kafka-async.producer
  (import org.apache.kafka.clients.producer.KafkaProducer)
  (import org.apache.kafka.clients.producer.ProducerRecord)
  (import java.util.UUID)
  (:require
   [kafka-async.commons :as commons]
   [clojure.core.async
    :as a
    :refer [>! <! >!! <!! go chan buffer close! thread
            alts! alts!! timeout go-loop]]
   ))

(def producers (atom {}))

(def producer-defaults
  {
   :retries "0"
   :batch.size "16384"
   :linger.ms "1"
   :buffer.memory "33554432"
   :key.serializer "org.apache.kafka.common.serialization.StringSerializer"
   :value.serializer "org.apache.kafka.common.serialization.StringSerializer"
   :acks "1"
   })

(defn- to-producer-record
  "TODO"
  [{topic :topic event :event}]
  (ProducerRecord. topic "" event))

(defn create
  "TODO - add description"
  [servers client-id]
  (let [producer (-> {:bootstrap.servers servers :client.id client-id}
                     (merge producer-defaults)
                     (commons/map-to-properties)
                     (KafkaProducer.))
        producer-id (.toString (UUID/randomUUID))
        in-chan (chan 4 (map to-producer-record))
        control-chan (chan)]
    (go-loop []
      (.send producer (<! in-chan))
      (recur))
    in-chan))

(defn close
  "Closes a Kafka Producer given a core.async chan"
  [chan]
  (println "I will close a Kafka Producer"))

