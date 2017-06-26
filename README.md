# kafka-async

A Clojure library to interact with Apache Kafka using a core.async interface.

## Usage

*kafka.async* has two major modules, producer and consumer.

### Producer

The producer API has two main functions, `create!` and `close`.

#### Producer creation

```clojure
(ns example
	(:require [kafka-async-producer :as producer]))
	
(defn example []
	(let [brokers "localhost:9092"
		  client-id "some-id"
		  {in-chan :chan producer-id :producer-id} (producer/create! brokers client-id)]
		(>!! in-chan {:topic "some-topic" :key "key" :event "Simple Kafka event"})))
```

#### Shuting down a producer

```clojure
(ns example
	(:require [kafka-async-producer :as producer]))
	
(producer/close "some-producer-id")
```

### Consumer

#### Consumer creation

```clojure
(ns example
	(:require [kafka-async-consumer :as consumer]))
	
(defn example []
	(let [brokers "localhost:9092"
		  group-id "some-id"
		  topics ["test"]
		  {out-chan :out-chan commit-chan :commit-chan consumer-id :consumer-id} (consumer/create! brokers group-id topics)]
	   (some-processor-fn (<!! out-chan))
	   (>!! commit-chan :kafka-commit)))
```



## License

Copyright © 2017 Joao Vazao Vasques

Distributed under the MIT Public License
