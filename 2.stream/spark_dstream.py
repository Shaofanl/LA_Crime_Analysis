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


if __name__ == '__main__':
    # spark context
    sc = pyspark.SparkContext(appName='Spark Streaming LA Crime')
    sc.setLogLevel("ERROR")

    # spark stream context
    ssc = StreamingContext(sc, 1)
    # kafka stream
    kafka_stream = KafkaUtils.createStream(
            ssc=ssc,
            zkQuorum="localhost:2181",
            groupId='spark-streaming',
            topics={'la-crime':1}
    )

    # from json string to object
    parsed = kafka_stream.map(lambda val: json.loads(val[1]))
    # print out the count
    parsed.count().map(lambda x: 'Number of crimes in the most recent batch: {}'.format(x)).pprint()

    parsed.map(lambda crime: crime['Crime Code Description'])\
            .countByValue()\
            .transform(lambda x: x.sortBy(lambda x: -x[1])).pprint()

    parsed.map(lambda crime: crime['Area Name'])\
            .countByValue()\
            .transform(lambda x: x.sortBy(lambda x: -x[1])).pprint()


    # start computation
    ssc.start()
    # wait for result
    ssc.awaitTermination()
