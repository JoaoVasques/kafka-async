(ns kafka-async.consumer
  (import org.apache.kafka.clients.consumer.KafkaConsumer)
  (import org.apache.kafka.clients.consumer.ConsumerRecords)
  (import java.util.ArrayList)
  (import java.util.UUID)
  (:require
   [kafka-async.commons :as commons]
   [clojure.core.async :as a :refer [>! <! close! chan timeout go-loop]]))

(def consumers
  "CLojure `atom` containing all the registered consumers in Kafka-Async"
  (atom {}))

(def consumer-defaults
  {
   :enable.auto.commit  "false"
   :auto.commit.interval.ms, "1000"
   :session.timeout.ms "30000"
   :key.deserializer "org.apache.kafka.common.serialization.StringDeserializer"
   :value.deserializer "org.apache.kafka.common.serialization.StringDeserializer"
   })

(defn records-by-topic
  "Higher order function that receives a set of consumer records and returns a function that expects a topic.

  The purpose of this returned function is to map each record in the set to a the topic passed as argument.

  The follwing data structure is returned:

  ```clojure
  {:topic ({:timestamp xxx :message \"some kafka message\"})}
  ```
  "
  {:doc/format :markdown}
  [records]
  (fn [topic]
    (let [consumer-records (.records records topic)]
    {(keyword topic)
     (map (fn [consumer-record]
            {:timestamp (.timestamp consumer-record)
             :message (.value consumer-record)})
      consumer-records)})))

(defn create!
   "Creates a Kafka Consumer client with a core.async interface given the broker's list and group id.

  After the Java Kafka consumer is created it's saved in the `consumers` atom with the following format:

  ```clojure
  {:uuid {:chan core-async-input-output-channel :commit-chan core-async-commit-chnannel :consumer java-kafka-consumer}}
  ```

  A core.async process is created that polls Kafka for messages and sends them to the output channel.

  Clients must manually send a message to the commit-chan in order to continue receiving messages
  by incrementing the consumer's offset.

  This function returns the following map to the client

  ```clojure
  {:out-chan out-chan :commit-chan commit-chan :consumer-id consumer-id}
  ```

  Usage example:

  ```clojure
  (let [{out-chan :out-chan commit-chan :commit-chan} (kafka-async-consumer/create! \"localhost:9092\" \"some-group-id\" [\"topic1\"])]
     (go-loop []
        (some-processor-fn (<! out-chan))
        (>! commit-chan :kafka-commit)
        (recur)))
  ```
  "
  {:doc/format :markdown}
  ([servers group-id topics] (create! servers group-id topics {}))
  ([servers group-id topics options]
   (let [consumer (-> {:bootstrap.servers servers :group.id group-id}
                      (merge consumer-defaults options)
                      (commons/map-to-properties)
                      (KafkaConsumer.))
         consumer-id (.toString (UUID/randomUUID))
         out-chan (chan)
         commit-chan (chan)]

     (.subscribe consumer (java.util.ArrayList. topics))

     (swap! consumers (fn [saved-consumers]
                        (let [info {(keyword consumer-id) {:chan out-chan :commit-chan commit-chan :consumer consumer}}]
                          (merge saved-consumers info))))

     (go-loop []
       (let [records (.poll consumer 1)
             records-per-topic (->> (reduce conj (map (records-by-topic records) topics))
                                    (into {} (filter (comp not-empty val)))
                                   )]
         (if-not (empty? records-per-topic)
           (do
             (>! out-chan records-per-topic)
             (<! commit-chan)
             (.commitSync consumer))
           (recur))))

     {:out-chan out-chan :commit-chan commit-chan :consumer-id consumer-id})))


(defn close
  "Closes a Kafka Consumer and the respective core-async channels given the consumer id obtained in `create!`.

  Returns `nil` if there is no consumer with the given id or a sequence of all consumers otherwise"
  [consumer-id]
  (let [{chan :chan commit-chan :commit-chan consumer :consumer} ((keyword consumer-id) @consumers)]
    (if-not (nil? (and chan consumer))
      (do
        (close! chan)
        (close! commit-chan)
        (.close consumer)
        (swap! consumers dissoc (keyword consumer-id)))
      (println "consumer not found"))))

