
"""
 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ spark-submit --jars \
      spark-streaming-kafka-0-8-assembly_*.jar \
      x42ws-process.py \
      localhost:9092 x42ws.public.data`
"""
from __future__ import print_function

import sys
import re
import json
import math

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import OffsetRange
from operator import add

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python x42ws-process.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="X42WeatherStationPythonStreamingDirectKafkaProcessor")
    ssc = StreamingContext(sc, 2)
    sqlContext = SQLContext(sc)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    jsonDStream = kvs.map(lambda (key, value): value)

    ###############
    # Globals
    ###############
    tempEx = 0.0
    tempEx2 = 0.0
    tempK = 0.0
    tempN = 0
    pressEx = 0.0
    pressEx2 = 0.0
    pressK = 0.0
    pressN = 0
    humidEx = 0.0
    humidEx2 = 0.0
    humidK = 0.0
    humidN = 0
    lightEx = 0.0
    lightEx2 = 0.0
    lightK = 0.0
    lightN = 0

    # Define functions to implement numerically stable version of incremental shifted variance calculation.
    # See article reference here: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
    #K = 0
    #n = 0
    #ex = 0
    #ex2 = 0

    def add_temp(x):
        global tempN, tempK, tempEx, tempEx2
        if (tempN == 0):
          tempK = x
        tempN = tempN + 1
        tempEx += x - tempK
        tempEx2 += (x - tempK) * (x - tempK)

    def get_temp_mean():
        global tempN, tempK, tempEx
        return tempK + tempEx / tempN

    def get_temp_variance():
        global tempN, tempEx, tempEx2
        return (tempEx2 - (tempEx*tempEx)/tempN) / (tempN-1)

    def get_temp_stddev():
        return math.sqrt(get_temp_variance())

    def add_press(x):
        global pressN, pressK, pressEx, pressEx2
        if (pressN == 0):
          pressK = x
        pressN = pressN + 1
        pressEx += x - pressK
        pressEx2 += (x - pressK) * (x - pressK)

    def get_press_mean():
        global pressN, pressK, pressEx
        return pressK + pressEx / pressN

    def get_press_variance():
        global pressN, pressEx, pressEx2
        return (pressEx2 - (pressEx*pressEx)/pressN) / (pressN-1)

    def get_press_stddev():
        return math.sqrt(get_press_variance())

    def add_humid(x):
        global humidN, humidK, humidEx, humidEx2
        if (humidN == 0):
          humidK = x
        humidN = humidN + 1
        humidEx += x - humidK
        humidEx2 += (x - humidK) * (x - humidK)

    def get_humid_mean():
        global humidN, humidK, humidEx
        return humidK + humidEx / humidN

    def get_humid_variance():
        global humidN, humidEx, humidEx2
        return (humidEx2 - (humidEx*humidEx)/humidN) / (humidN-1)

    def get_humid_stddev():
        return math.sqrt(get_humid_variance())

    def add_light(x):
        global lightN, lightK, lightEx, lightEx2
        if (lightN == 0):
          lightK = x
        lightN = lightN + 1
        lightEx += x - lightK
        lightEx2 += (x - lightK) * (x - lightK)

    def get_light_mean():
        global lightN, lightK, lightEx
        return lightK + lightEx / lightN

    def get_light_variance():
        global lightN, lightEx, lightEx2
        return (lightEx2 - (lightEx*lightEx)/lightN) / (lightN-1)

    def get_light_stddev():
        return math.sqrt(get_light_variance())

    # Define function to process RDDs of the json DStream to convert them
    #   to DataFrame and run SQL queries
    def process(time, rdd):
        print("========= %s =========" % str(time))

        try:
            # Parse the one line JSON string from the DStream RDD
	    jsonString = rdd.map(lambda x: \
		re.sub(r"\s+", "", x, flags=re.UNICODE)).reduce(add)
	    print("jsonString = %s" % str(jsonString))

            # Convert the JSON string to an RDD
            jsonRDDString = sc.parallelize([str(jsonString)])

            # Convert the JSON RDD to Spark SQL Context
	    jsonRDD = sqlContext.read.json(jsonRDDString)

            # Register the JSON SQL Context as a temporary SQL Table
	    print("JSON Schema\n=====")
            jsonRDD.printSchema()
            jsonRDD.registerTempTable("iotmsgsTable")

            #############
            # Processing and Analytics go here
            #############

            # Separate the sensor value types
            tempValues = sqlContext.sql("select payload.data.temperature from iotmsgsTable")
            tempValues.show(n=100)
            pressValues = sqlContext.sql("select payload.data.pressure from iotmsgsTable")
            pressValues.show(n=100)
            humidValues = sqlContext.sql("select payload.data.humidity from iotmsgsTable")
            humidValues.show(n=100)
            lightValues = sqlContext.sql("select payload.data.ambient_light from iotmsgsTable")
            lightValues.show(n=100)

            # Process statistics
            tempValuesList = tempValues.collect()
            for x in tempValuesList:
                for key in x:
                    print("Key", key)
                add_temp(x.temperature)
            tempMean = get_temp_mean()
            tempStd = get_temp_stddev()
            print("Temperature - mean=", tempMean, ", std.dev=", tempStd)
            tempThreshLow = tempMean - tempStd
            tempThreshHigh = tempMean + tempStd
            print("Low temp is <", tempThreshLow, "and high temp is >", tempThreshHigh)
            #pressValues.foreach(add_press)
            #humidValues.foreach(add_humid)
            #lightValues.foreach(add_light)

            # Process triggers
            # I want to compare the current value with thresholds set up as mean +/- 2 std-dev and if not in between, trigger first
            # Eventually, if we keep track of this for each device, we can do things like get ensemble stats and notice how many are outside that range too
            # Some reporting could also include nearest neighbors stats, etc.
            # But first we gotta set up the stats! Why isn't it working?

            # Clean-up
	    sqlContext.dropTempTable("iotmsgsTable")
        # Catch any exceptions
        except Exception as e:
            print("Processing exception: %s" % e)

    # Process each RDD of the DStream coming in from Kafka
    jsonDStream.foreachRDD(process)
 
    ssc.start()
    ssc.awaitTermination()
