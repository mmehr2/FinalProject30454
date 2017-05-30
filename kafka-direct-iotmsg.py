#

"""
 Processes direct stream from kafka, '\n' delimited text directly received 
   every 2 seconds.
 Usage: kafka-direct-iotmsg.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a 
   producer first, see:
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-direct-iotmsg.py \
      localhost:9092 iotmsgs`
"""
from __future__ import print_function

import sys
import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from operator import add

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka-direct-iotmsg.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # Read in the Kafka Direct Stream into a TransformedDStream
    lines = kvs.map(lambda x: x[1])
    jsonLines = lines.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE)) 

    ############
    # 
    # Processing
    #
    ############ 

    # Search for specific IoT data values (assumes jsonLines are split(','))
    dataValues = jsonLines.filter(lambda x: re.findall(r"temperature.*", x, 0)) 
    dataValues.pprint(num=10000)

    # Parse out just the value without the JSON key
    parsedValues = dataValues.map(lambda x: re.sub(r"\"temperature\":", "", x))

    # Count how many values were parsed
    countMap = parsedValues.map(lambda x: 1).reduce(add)
    valueCount = countMap.map(lambda x: "Total Count of Msgs: " + unicode(x))
    valueCount.pprint()

    # Sort all the IoT values
    sortedValues = parsedValues.transform(lambda x: x.sortBy(lambda y: y))
    sortedValues.pprint(num=10000)

    ssc.start()
    ssc.awaitTermination()

