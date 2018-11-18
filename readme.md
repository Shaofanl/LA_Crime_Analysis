LA Crime Data Analysis
---------------------

![Pipeline](http://shaofanlai.com/archive/storage/kloo4Y82QxqhgS8RcMxaygVGnmFs5c9406pN4BSK9aMIuVnOao)

This is a data science practice that focuses on the crime analysis in LA with Apache family (**Spark/Kafka/Hadoop/HDFS/Yarn/Zookeeper/Flink**).

I have been living in LA for over one year and I received some safety warnings from our Department of Public Safety from time to time. I feel necessary to know about how the crimes distributed in LA so that I don't have to worry that much when I am out for dinner.

All projects are produced by myself and feel welcome to comment in the issue section.
- 0. Setup (To handle big data with distributed systems)
  - Build an extensible cluster of 2 workers and 1 master with [VMWare](http://shaofanlai.com/post/87).
  - (optional) Configured the distributed environment for **HDFS** and **Spark** and used **Yarn** to manage resources.
- 1. Batch Analysis (To live more safely by better understanding of the crime distribution)
  - Stored the large dataset on a distributed file system with **HDFS** and manage the cluster with **Yarn**.
  - Preprocessed the raw data, cleaned the anomaly, filtering targeted data, and calculate statistics with **Spark**.
  - Visualized the crime trend and distribution among with different attributions (e.g. time/gender/year/location) with **Seaborn** and **Matplotlib**.
  - Clustered the center of aggressive crimes and get a better understanding of the safety of neighborhood with **KMeans** in **Spark MLLib**.
Please check the notebook for more details.
- 2\_1. Streaming Analysis (To build a real-time crime alarming system)
  - Designed a flexible simulation framework that transform a batch file to streams according to the time.
  - Directed the streaming data come from the producer to **Spark** with **Kafka**.
  - Analyzed the number of crimes in LA, number of crimes in my neighborhood, top-5 crime types, top-5 crime locations with **Spark DStream**.
  - Calculated the statistics on different time-level (last hour/last 6 hours/last 12 hours) with **Spark Windowed Streaming**
  - Optimized the parsing procedure of structured stream for a better performance with the **Spark Structured Streaming**.
- 2\_2. Streaming with Flink
  - Developed a streaming pipeline to calculate the recently reported crimes for different kinds with **Flink** in **scala**.
 
Happy Coding and Live Safely :)
