import findspark
findspark.init()
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.2 pyspark-shell'

# spark-streaming-kafka-0-10 hasn't support Python yet
# https://spark.apache.org/docs/latest/streaming-kafka-integration.html


import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import json
import numpy as np
from scipy.spatial.distance import euclidean

USC = np.array([34.021861, -118.282942])

if __name__ == '__main__':
    def setup_func():
        from pyspark.sql.functions import col
        # spark context
        sc = pyspark.SparkContext(appName='Spark Streaming LA Crime')
        sc.setLogLevel("ERROR")

        # spark stream context
        ssc = StreamingContext(sc, 1)
        ssc.checkpoint('/tmp/checkpoint-v7')
        # kafka stream
        kafka_stream = KafkaUtils.createStream(
                ssc=ssc,
                zkQuorum="localhost:2181",
                groupId='spark-streaming',
                topics={'la-crime':1}
        )

        # from json string to object
        parsed = kafka_stream.map(lambda val: json.loads(val[1]))

        # print out the count in this batch
        parsed.count().map(lambda x: 'Number of crimes in last hour: {}'.format(x)).pprint()

        parsed.map(lambda crime: euclidean(USC, list(map(float, crime['Location '][1:-1].split(','))) )\
                                    if crime['Location ']else float('inf') )\
                .filter(lambda x: x < 0.02)\
                .count()\
                .map(lambda x: 'Number of crimes near USC: {}'.format(x))\
                .pprint()

        parsed.map(lambda crime: crime['Crime Code Description'])\
                .countByValue()\
                .transform(lambda x: x.sortBy(lambda x: -x[1]))\
                .pprint(5)
                # .transform(lambda rdd: sc.parallelize(rdd.take(5)))\

        parsed.map(lambda crime: crime['Area Name'])\
                .countByValue()\
                .transform(lambda x: x.sortBy(lambda x: -x[1]))\
                .pprint(5)
                # .transform(lambda rdd: sc.parallelize(rdd.take(5)))\


        # print out the count last 6 batch 
        parsed.countByWindow(6, 1).map(lambda x: 'Number of crimes in last 6 hours: {}'.format(x)).pprint()

        parsed.map(lambda crime: euclidean(USC, list(map(float, crime['Location '][1:-1].split(','))) )\
                                    if crime['Location ']else float('inf') )\
                .filter(lambda x: x < 0.02)\
                .countByWindow(6, 1)\
                .map(lambda x: 'Number of crimes near USC in last 6 hours: {}'.format(x))\
                .pprint()

        parsed.map(lambda crime: crime['Crime Code Description'])\
                .countByValueAndWindow(6, 1)\
                .transform(lambda x: x.sortBy(lambda x: -x[1]))\
                .pprint(5)
                # .transform(lambda rdd: sc.parallelize(rdd.take(5)))\

        parsed.map(lambda crime: crime['Area Name'])\
                .countByValueAndWindow(6, 1)\
                .transform(lambda x: x.sortBy(lambda x: -x[1]))\
                .pprint(5)
                # .transform(lambda rdd: sc.parallelize(rdd.take(5)))\


        # print out the count last 12 batch 
        parsed.countByWindow(12, 1).map(lambda x: 'Number of crimes in last 12 hours: {}'.format(x)).pprint()

        parsed.map(lambda crime: euclidean(USC, list(map(float, crime['Location '][1:-1].split(','))) )\
                                    if crime['Location ']else float('inf') )\
                .filter(lambda x: x < 0.02)\
                .countByWindow(12, 1)\
                .map(lambda x: 'Number of crimes near USC in last 12 hours: {}'.format(x))\
                .pprint()

        parsed.map(lambda crime: crime['Crime Code Description'])\
                .countByValueAndWindow(12, 1)\
                .transform(lambda x: x.sortBy(lambda x: -x[1]))\
                .pprint(5)
                # .transform(lambda rdd: sc.parallelize(rdd.take(5)))\

        parsed.map(lambda crime: crime['Area Name'])\
                .countByValueAndWindow(12, 1)\
                .transform(lambda x: x.sortBy(lambda x: -x[1]))\
                .pprint(5)
                # .transform(lambda rdd: sc.parallelize(rdd.take(5)))\
        return ssc



    # start computation
    ssc = setup_func()
    ssc.start()
    # wait for result
    ssc.awaitTermination()
