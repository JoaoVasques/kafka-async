(ns kafka-async.producer
  (import org.apache.kafka.clients.producer.KafkaProducer)
  (import org.apache.kafka.clients.producer.ProducerRecord)
  (import java.util.UUID)
  (:require
   [kafka-async.commons :as commons]
   [clojure.core.async :as a :refer [>! <! >!! close! chan timeout go-loop]]))

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

(def default-in-chan-size
  "Producer core.async input channel default buffer size"
  100)

(defn to-producer-record
  "Converts a map into a Kafka Producer Record. The map contains the following structure

  ```clojure
  {:topic \"some-topic\" :hash-key \"some-hash\" :event {...}}
  ```
  "
  {:doc/format :markdown}
  [{topic :topic hash-key :key event :event}]
  (ProducerRecord. topic hash-key event))

(defn create!
  "Creates a Kafka Producer client with a core.async interface given the broker's list and group id.

  After the Java Kafka producer is created it's saved in the `producers` atom with the following format:

  ```clojure
  {:uuid {:chan core-async-input-channel :producer java-kafka-producer}}
  ```

  A core.async process is created that reads from the input channel and sends the event to Java Kafka Producer.
  If a nil event is passed the process ends.

  This function returns the following map to the client

  ```clojure
  {:chan in-chan :id producer-id}
  ```
  "
  {:doc/format :markdown}
  ([servers client-id] (create! servers client-id default-in-chan-size {}))
  ([servers client-id buffer-size options]
  (let [producer (-> {:bootstrap.servers servers :client.id client-id}
                     (merge producer-defaults options)
                     (commons/map-to-properties)
                     (KafkaProducer.))
        producer-id (.toString (UUID/randomUUID))
        in-chan (chan buffer-size (map to-producer-record))]

    (swap! producers (fn [saved-producers]
                       (let [info {(keyword producer-id) {:chan in-chan :producer producer}}]
                         (merge saved-producers info))))
    (go-loop []
      (let [event (<! in-chan)]
        (if-not (nil? event)
          (do
            (.send producer event)
            (recur)
            ))))
    {:chan in-chan :id producer-id})))

(defn close
  "Closes a Kafka producer and the respective core-async channel given the producer id obtained in `create!`.

  Returns `nil` if there is no producer with the given id or a sequence of all producers otherwise"
  [producer-id]
  (let [{chan :chan producer :producer} ((keyword producer-id) @producers)]
    (if-not (nil? (and chan producer))
      (do
        (close! chan)
        (.close producer)
        (swap! producers dissoc (keyword producer-id)))
      (println "producer not found"))))

