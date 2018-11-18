import findspark
findspark.init()
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 pyspark-shell'


import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import json
import numpy as np
from scipy.spatial.distance import euclidean

USC = np.array([34.021861, -118.282942])

if __name__ == '__main__':
    spark = pyspark.sql.SparkSession \
            .builder \
            .appName("StructuredNetworkWordCount") \
            .getOrCreate() 

    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094") \
      .option("subscribe", "la-crime") \
      .load()

    from pyspark.sql.functions import get_json_object, decode
    df_string = df.select(decode(df.value, 'UTF-8').alias('json'))  # binary to UTF-8
    crime_types = df_string.select(get_json_object(df_string.json, '$.Crime Code Description').alias('types'))
    crime_types_count = crime_types.groupBy("types").count().orderBy('count', ascending=False)  # .limit(5)

    query = crime_types_count\
        .writeStream \
        .outputMode("complete")\
        .format("console") \
        .start()

    query.awaitTermination()
