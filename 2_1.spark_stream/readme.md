2.stream
---

In this part, we will analyze the crime reports in real-time stream with Apache Kafka and Apache Spark (Dstream, Windowed Stream, Structured Stream) in Python3.
In order to do that, we will first build a streaming system to simulate the report stream of crimes.
Although the reported date is given, the exact timetag is not and here we assume that the police office is efficient enough, hopefully, that crimes of type `ASSAULT`, `BURGLARY`, or `ROBBERY` can be reported to the system in 30 minutes to 90 minutes.

Table of Contents
---
We are going to:
- Setup the Kafka and Zookeeper.
- Build a stream simulator(s).
- Connect Kafka with Spark.
- Use Spark Streaming to do real-time analysis.
- Try out Spark Structured Streaming.

Kafka and Zookeeper
---
Please refer to the [official documents](https://kafka.apache.org/) to setup the Kafka and Zookeeper. I used 3 brokers and the description looks like:
```
Topic:la-crime  PartitionCount:1        ReplicationFactor:3     Configs:
        Topic: la-crime Partition: 0    Leader: 0       Replicas: 0,2,1 Isr: 0,2,1
```

Build stream simulator(s)
---
I designed a `CrimeStreamer` that can: 
1. Filter the crime of interest (`ASSAULT`, `BURGLARY`, `ROBBERY`).
2. Simulate the report time (30~90 minutes after it commited).
3. Sort and Partition all records into one or many parts randomly. Each partition represents a police officer.
If the batch file can be fitted in your memory, you can use `BasicCrimeStreamer` directly. Otherwise, you can inherit `CrimeStreamer` and implement your own API to build a streamer that can handle larger dataset. 

Connect Kafka with Spark.
---
We can keep using python and there is [a python library for Kafka](https://github.com/Parsely/pykafka).
Also notice that we have to use ` spark-streaming-kafka-0-8` (deprecated) since `spark-streaming-kafka-0-10` hasn't supported Python yet.

Spark DStream
---
In `spark_dstream.py`, I set the `acceleration` to 3\*60\*60, which means 1 second in the real-life equals to 3 hours in the simulation. From the stream, we use Spark Stream to calculate:
- the total number of crimes
- the number of crimes near USC
- top-5 crime type
- top-5 crime location

Spark Windowed Stream
---
In `spark_windowed.py`, I set the `acceleration` to 60\*60, which means 1 second in the real-life equals to 1 hour in the simulation.
From the stream, we use Spark Windowed Stream to calculate the same statistics but in different time level (1 hour/6 hours/12 hours).

Spark Structured Streaming
---
Spark Structured Streaming can read the input stream as a structured/relationship table. 
First, we need to change the required package from `spark-stream-kafka` to `spark-sql-kafka`.
And Note that we have to transform binary data from Kafka first before we move to calculation.

To be honest, the structured streaming is still undeveloped. There are a lot of common operations (e.g. limit) are [unsupported](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations) and you can easily trigger a bug since the steaming DataFrame is different from the normal DataFrame.

Notes on Kafka
---
Apache Kafka can be used to decouple the data pipeline:
- Producer: app that publish message to a topic
- Consumer: app that subscribes to a topic and consume the messages
- Broker: each server in the cluster
  - leader, follower: when leader has some error, zookeeper will elect a follower as the new leader
- Topic: a category of feed name to which records are published
- Partition: topics are broken up into ordered commit logs called partitions
- ZooKeeper: managing and coordinating Kafka broker
Commands:
- `kafka-server-start.sh` is used to control brokers
- `kafka-topic.sh` is used to manage topics 
Resources:
- [Spark stream with Kafka](https://www.rittmanmead.com/blog/2017/01/getting-started-with-spark-streaming-with-python-and-kafka/)
- [Structural Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
