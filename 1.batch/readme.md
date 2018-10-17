1.Batch Analysis
---

### Data Collection 
Data can be downloaded from the [official website](https://data.lacity.org/A-Safe-City/Crime-Data-from-2010-to-Present/y8tr-7khq). Click the `view data`, `export`, `download`, and `csv` to download the batch file. 
If this link is invalid, search for `LA crime database` with Google and download the csv file.

If you want to store the csv file to the HDFS, you can run `hdfs dfs -mkdir crime && hdfs dfs -put crime.csv crime/` to sync with my code.

I used a [**HDFS+Yarn+Spark** architecture in Client mode](https://databricks.com/blog/2014/01/21/spark-and-hadoop.html) with a cluster of 3. It approximates the industrial environment and is highly scalable.
[This tutorial](https://www.linode.com/docs/databases/hadoop/install-configure-run-spark-on-top-of-hadoop-yarn-cluster/) is useful to bridge Yarn and Spark (which is hard to configure).

Notice: set the #vcore and #memory to a small number in the simulated environment.

### Analysis
- Stored the large dataset on a distributed file system with **Apache HDFS** and manage the cluster with **Apache Yarn**.
- Preprocessed the raw data, cleaned the anomaly, filtering targeted data, and calculate statistics with **Apache Spark**.
- Visualized the crime trend and distribution among with different attributions (e.g. time/gender/year/location) with **Seaborn** and **Matplotlib**.
- Clustered the center of aggressive crimes and get a better understanding of the safety of neighborhood with **KMeans** in **Spark MLLib**.
Please check the notebook for more details.
