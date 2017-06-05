
"""
 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ spark-submit --jars \
      spark-streaming-kafka-0-8-assembly_*.jar \
      x42ws-process.py \
      localhost:9092 x42ws.public.data`

Reported JSON schema of current data format urn:com:azuresults:x42ws:sensors:
JSON Schema
=====
root
 |-- destination: string (nullable = true)
 |-- eventTime: string (nullable = true)
 |-- guid: string (nullable = true)
 |-- payload: struct (nullable = true)
 |    |-- data: struct (nullable = true)
 |    |    |-- ambient_light: double (nullable = true)
 |    |    |-- humidity: double (nullable = true)
 |    |    |-- pressure: double (nullable = true)
 |    |    |-- temperature: double (nullable = true)
 |    |    |-- timestamp: string (nullable = true)
 |    |-- format: string (nullable = true)

Memory leak detection via pympler:
    pip install pympler
See here for docs: https://pythonhosted.org/Pympler/

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
from pympler import tracker

if __name__ == "__main__":
  try:
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
    mTracker = None

    print("Restoring statistics here, if any ...")

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
        if (tempN == 0):
            return 0.0
        return tempK + tempEx / tempN

    def get_temp_variance():
        global tempN, tempEx, tempEx2
        if (tempN > 2):
            return 0.0
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
        if (pressN == 0):
            return 0.0
        return pressK + pressEx / pressN

    def get_press_variance():
        global pressN, pressEx, pressEx2
        if (pressN > 2):
            return 0.0
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
        if (humidN == 0):
            return 0.0
        return humidK + humidEx / humidN

    def get_humid_variance():
        global humidN, humidEx, humidEx2
        if (humidN > 2):
            return 0.0
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
        if (lightN == 0):
            return 0.0
        return lightK + lightEx / lightN

    def get_light_variance():
        global lightN, lightEx, lightEx2
        if (lightN > 2):
            return 0.0
        return (lightEx2 - (lightEx*lightEx)/lightN) / (lightN-1)

    def get_light_stddev():
        return math.sqrt(get_light_variance())

    def showerr(guid, typestr, val, thrL, thrH):
        if val > thrH:
            print("****************************************")
            print("**** DEVICE", guid, typestr, val, "is over upper threshold", thrH)
            print("****************************************")
        elif val < thrL:
            print("****************************************")
            print("**** DEVICE", guid, typestr, val, "is under lower threshold", thrL)
            print("****************************************")

    # Define function to process RDDs of the json DStream to convert them
    #   to DataFrame and run SQL queries
    def process(time, rdd):
        global mTracker
        #if mTracker == None:
        #    mTracker = tracker.SummaryTracker()
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
            tempValues = sqlContext.sql("select guid, payload.data.temperature as temperature from iotmsgsTable")
            tempValues.show(n=100)
            pressValues = sqlContext.sql("select guid, payload.data.pressure as pressure from iotmsgsTable")
            pressValues.show(n=100)
            humidValues = sqlContext.sql("select guid, payload.data.humidity as humidity from iotmsgsTable")
            humidValues.show(n=100)
            lightValues = sqlContext.sql("select guid, payload.data.ambient_light as ambient_light from iotmsgsTable")
            lightValues.show(n=100)

            # Process statistics
            #tempValues.foreach(lambda x: add_temp(x.temperature))
            tempValuesList = tempValues.collect()
            for x in tempValuesList:
                add_temp(x.temperature)
            tempMean = get_temp_mean()
            tempStd = get_temp_stddev()
            print("Temperature - mean=", tempMean, ", std.dev=", tempStd)
            tempThreshLow = tempMean - tempStd * 2.0
            tempThreshHigh = tempMean + tempStd * 2.0
            print("Two-sigma Low temp is <", tempThreshLow, "and high temp is >", tempThreshHigh)
            #pressValues.foreach(lambda x: add_press(x.pressure))
            pressValuesList = pressValues.collect()
            for x in pressValuesList:
                add_press(x.pressure)
            pressMean = get_press_mean()
            pressStd = get_press_stddev()
            print("Barometric Pressure - mean=", pressMean, ", std.dev=", pressStd)
            pressThreshLow = pressMean - pressStd * 2.0
            pressThreshHigh = pressMean + pressStd * 2.0
            print("Two-sigma Low pressure is <", pressThreshLow, "and high pressure is >", pressThreshHigh)
            #humidValues.foreach(lambda x: add_humid(x.humidity))
            humidValuesList = humidValues.collect()
            for x in humidValuesList:
                add_humid(x.humidity)
            humidMean = get_humid_mean()
            humidStd = get_humid_stddev()
            print("Humidity - mean=", humidMean, ", std.dev=", humidStd)
            humidThreshLow = humidMean - humidStd * 2.0
            humidThreshHigh = humidMean + humidStd * 2.0
            print("Two-sigma Low humid is <", humidThreshLow, "and high humid is >", humidThreshHigh)
            #lightValues.foreach(lambda x: add_light(x.ambient_light))
            lightValuesList = lightValues.collect()
            for x in lightValuesList:
                add_light(x.ambient_light)
            lightMean = get_light_mean()
            lightStd = get_light_stddev()
            print("Ambient Light Level - mean=", lightMean, ", std.dev=", lightStd)
            lightThreshLow = lightMean - lightStd * 2.0
            lightThreshHigh = lightMean + lightStd * 2.0
            print("Two-sigma Low light is <", lightThreshLow, "and high light is >", lightThreshHigh)

            # Process triggers
            # I want to compare the current value with thresholds set up as mean +/- 2 std-dev and if not in between, trigger first
            # Eventually, if we keep track of this for each device, we can do things like get ensemble stats and notice how many are outside that range too
            # Some reporting could also include nearest neighbors stats, etc.
            for x in tempValuesList:
                print("Device", x.guid, ", temperature", x.temperature)
                showerr(x.guid, "TEMPERATURE", x.temperature, tempThreshLow, tempThreshHigh)
            for x in pressValuesList:
                showerr(x.guid, "PRESSURE", x.pressure, pressThreshLow, pressThreshHigh)
            for x in humidValuesList:
                showerr(x.guid, "HUMIDITY", x.humidity, humidThreshLow, humidThreshHigh)
            for x in lightValuesList:
                showerr(x.guid, "LIGHT LEVEL", x.ambient_light, lightThreshLow, lightThreshHigh)
            
            # Clean-up
	    sqlContext.dropTempTable("iotmsgsTable")
        # Catch any exceptions
        except KeyboardInterrupt:
            print("Persisting stats data 3...") # prob.needs transaction semantics to avoid partial saves tho
        except Exception as e:
            print("Processing exception: %s" % e)
        #finally:
            # Track memory leaks
            #mTracker.print_diff()

    # Process each RDD of the DStream coming in from Kafka
    jsonDStream.foreachRDD(process)
 
    ssc.start()
    ssc.awaitTermination()
  except KeyboardInterrupt:
      print("Persisting stats data2...")
  except Exception as e:
      # Persist data here
      print("Main loop exception %s" % e)
      print("Persisting stats data1...")
