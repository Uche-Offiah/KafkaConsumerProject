# KafkaConsumerProject
--Start ZooKeeper
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

--start kafka-server
.\bin\windows\kafka-server-start.bat .\config\server.properties

-- a consumer and producer resides in the windows folder and there you should navigate with cmd
kafka-topics.bat --create --bootstrap-server localhost:9092 --topic test-topic

-- Create a producer
kafka-console-producer.bat --broker-list localhost:9092 --topic test

-- create a consumer
kafka-console-consumer.bat --topic test --bootstrap-server localhost:9092 --from-beginning


--Stop ZooKeeper
.\bin\windows\zookeeper-server-stop.bat .\config\zookeeper.properties

--Stop Kafka-server
.\bin\windows\kafka-server-stop.bat .\config\server.properties

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-topic