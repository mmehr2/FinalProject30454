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

running_total_H = 0
running_count_H = 0
running_total_L = 0
running_count_L = 0

def addstatH(value_str):
    global running_total_H, running_count_H
    running_count_H += 1
    temp = float(value_str)
    running_total_H += temp
    return temp

def addstatL(value_str):
    global running_total_L, running_count_L
    running_count_L += 1
    temp = float(value_str)
    running_total_L += temp
    return temp

def running_mean(count, total):
    if count == 0:
        return 0.0
    return (total / count)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka-direct-iotmsg.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaIoTMessageAnalysis")
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
    dataValuesH = jsonLines.filter(lambda x: re.findall(r"humidity.*", x, 0))
    dataValuesH.pprint(num=10000)
    dataValuesL = jsonLines.filter(lambda x: re.findall(r"ambient_light.*", x, 0))
    dataValuesL.pprint(num=10000)

    # Parse out just the value without the JSON key
    parsedValuesH = dataValuesH.map(lambda x: re.sub(r"\"humidity\":", "", x))
    parsedValuesL = dataValuesL.map(lambda x: re.sub(r"\"ambient_light\":", "", x))

    # Count how many values were parsed
    countMap = parsedValuesH.map(lambda x: 1).reduce(add)
    valueCount = countMap.map(lambda x: "Total Count of Msgs: " + unicode(x))
    valueCount.pprint()

    # Keep running statistics for substreams
    sumMapH = parsedValuesH.map(addstatH).reduce(add)
    sumMapL = parsedValuesL.map(addstatL).reduce(add)
    valueCountH = sumMapH.map(lambda x: "Total Sum of Humidity: " + unicode(x))
    valueCountL = sumMapL.map(lambda x: "Total Sum of Light: " + unicode(x))

    # Sort all the IoT values
    sortedValuesH = parsedValuesH.transform(lambda x: x.sortBy(lambda y: y))
    sortedValuesL = parsedValuesL.transform(lambda x: x.sortBy(lambda y: y))
    sortedValuesH.pprint(num=10000)
    sortedValuesL.pprint(num=10000)

    print("Running stats for Humidity: count=%d total=%.2f mean=%.2f" % (running_count_H, running_total_H, \
                                                                        running_mean(running_count_H, running_total_H) ) )
    print("Running stats for Ambient Light: count=%d total=%.2f mean=%.2f" % (running_count_L, running_total_L, \
                                                                        running_mean(running_count_L, running_total_L) ) )

    ssc.start()
    ssc.awaitTermination()

