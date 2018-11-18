Flink Streaming
----

Achieving the similar task as 2\_1 with Apache Flink. Flink is a real streaming system compared to the mini-batching of Spark. Unfortunately, even in the latest version of Flink (1.6.2), the Python API doesn't support streaming from Kafka. And streaming from a socket in a customized stream source can cause an error when executed by PyFlink. Therefore, this part is written by **Scala** and compiled with **Maven**.


- Used the **Flink** to analyze the recently reported crimes of every categories.
- This part is still in development.
