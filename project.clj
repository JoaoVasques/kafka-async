(defproject kafka-async "0.1.0"
  :description "Core.async library for Apache Kafka"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.3.442"]
                 [org.apache.kafka/kafka_2.11 "0.10.1.0"]
                 ]
  :plugins [[lein-codox "0.10.3"]])
