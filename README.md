Repository contains code for apache flink demo example

How to run:
sbt package

Note : Before using this project you should install sbt,scala.
Also, before running flink table api you should explicitly download flink-table_2.12-1.7.2.jar file from maven repository 
and put in your classpath.It is open issue in flink while building all dependencies for flink table dependencies.

Download link : http://central.maven.org/maven2/org/apache/flink/flink-table_2.12/1.7.2/

If you are using scala version 2.11 then download 2.11 flink table jar.

Use below command to test kafka and flink example:
Before running the kafka-flink example run below comamnd in your environment

1)Create the topic which will use for communication:

Create the topic in kafka: \
kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test

2)List the topic present in kafka:

List the kafka topic: \
kafka-topics.bat --list --bootstrap-server localhost:9092

3)Produce(Publish) the message in kafka topic:

Command to publish the message in kafka topic:\
kafka-console-producer.bat --broker-list localhost:9092 --topic test

4)Subscribe(Read) the message from kafka topic:

Command to receive the message from kafka topic: \
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning

