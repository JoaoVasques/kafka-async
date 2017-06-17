(ns kafka-async.consumer
  (import org.apache.kafka.clients.consumer.KafkaConsumer)
  (import org.apache.kafka.clients.consumer.ConsumerRecords)
  (import java.util.ArrayList)
  (import java.util.UUID)
  (:require
   [kafka-async.commons :as commons]
   [clojure.core.async :as a :refer [>! <! close! chan timeout go-loop]]))

(def consumers (atom {}))

(def consumer-defaults
  {
   :enable.auto.commit  "false"
   :auto.commit.interval.ms, "1000"
   :session.timeout.ms "30000"
   :key.deserializer "org.apache.kafka.common.serialization.StringDeserializer"
   :value.deserializer "org.apache.kafka.common.serialization.StringDeserializer"
   })

(defn- records-by-topic
  ""
  {:doc/format :markdown}
  [consumer poll-size]
  (fn [topic]
    (let [consumer-records (-> (.poll consumer poll-size)
                               (.records topic))]
    {(keyword topic)
     (map (fn [consumer-record]
            {:timestamp (.timestamp consumer-record)
             :message (.value consumer-record)})
      consumer-records)})))

(defn create!
  ""
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
       (let [records-per-topic (->> (reduce conj (map (records-by-topic consumer 1) topics))
                                   (into {} (filter (comp not-empty val)))
                                   )]
         (if-not (empty? records-per-topic)
           (do
             (>! out-chan records-per-topic)
             (<! commit-chan)
             (.commitSync consumer))
           (recur))))

     {:out-chan out-chan :commit-chan commit-chan})))

