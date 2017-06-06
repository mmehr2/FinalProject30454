
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
import requests

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
    # WARNING STATES  -1: low warning, +1: high warning, 0: ok, 2: turning off
    warnStateTemp = 0
    warnStatePress = 0
    warnStateHumid = 0
    warnStateLight = 0
    alarm = 0 # States: 0=off, 1=on, 2=chg to off, 3=chg to on

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
        if (tempN < 2):
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
        if (pressN < 2):
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
        if (humidN < 2):
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
        if (lightN < 2):
            return 0.0
        return (lightEx2 - (lightEx*lightEx)/lightN) / (lightN-1)

    def get_light_stddev():
        return math.sqrt(get_light_variance())

    def procstats(x):
            add_temp(x.temperature)
            add_press(x.pressure)
            add_humid(x.humidity)
            add_light(x.ambient_light)

    def showerr(guid, typestr, val, thrL, thrH):
        global sensorStates
        old_state = get_sensor_states()[typestr]
        new_state = old_state
        if val > thrH:
            print("****************************************")
            print("**** DEVICE", guid, typestr, val, "is over upper threshold", thrH)
            print("****************************************")
            new_state = 1
        elif val < thrL:
            print("****************************************")
            print("**** DEVICE", guid, typestr, val, "is under lower threshold", thrL)
            print("****************************************")
            new_state = -1
        else:
            if old_state != 0:
                new_state = 2 #transitioning to off
                print("****************************************")
                print("**** DEVICE", guid, typestr, val, "is back in range.")
                print("****************************************")
            elif old_state == 2:
                new_state = 0
        set_sensor_states(typestr, new_state)

    def get_sensor_states():
        global warnStateTemp, warnStatePress, warnStateHumid, warnStateLight
        return { "TEMPERATURE": warnStateTemp,
                 "PRESSURE": warnStatePress,
                 "HUMIDITY": warnStateHumid,
                 "LIGHT": warnStateLight }

    def set_sensor_states(typestr, new_state):
        global warnStateTemp, warnStatePress, warnStateHumid, warnStateLight
        if typestr == "TEMPERATURE":
            warnStateTemp = new_state
        if typestr == "PRESSURE":
            warnStatePress = new_state
        if typestr == "HUMIDITY":
            warnStateHumid = new_state
        if typestr == "LIGHT":
            warnStateLight = new_state

    def calc_alarm_type():
        '''If any sensor warning state is 1 or -1, we are in alarm state, return True, else False. '''
        global warnStateTemp, warnStatePress, warnStateHumid, warnStateLight
        if warnStateTemp == -1 or warnStateTemp == 1:
            return "TEMPERATURE"
        if warnStatePress == -1 or warnStatePress == 1:
            return "PRESSURE"
        if warnStateHumid == -1 or warnStateHumid == 1:
            return "HUMIDITY"
        if warnStateLight == -1 or warnStateLight == 1:
            return "LIGHT"
        return ""

    def trigger_alarm():
        global alarm
        old_alarm = alarm
        new_alarm = old_alarm
        typestr = calc_alarm_type()
        state_alarm = (typestr != "")
        do_send = False
        if state_alarm:
            if old_alarm == 0:
                new_alarm = 3 #setting ON
                do_send = True
            elif old_alarm == 1:
                pass # staying ON
            elif old_alarm == 2:
                pass #changed mind??
            elif old_alarm == 3:
                do_send = True # still trying to set ON
        else:
            if old_alarm == 0:
                pass # staying OFF
            elif old_alarm == 1:
                new_alarm = 2 #setting OFF
                do_send = True
            elif old_alarm == 2:
                do_send = True # still trying to set OFF
            elif old_alarm == 3:
                pass # changed mind??
        alarm = new_alarm
        if do_send:
            send_alarm(typestr)

    def send_alarm(typestr):
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        global alarm
        state = alarm
        if state < 2:
            return
        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
        print("################################")
        print("################################")
        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        url = "https://893639c5.ngrok.io/weather/api" #  From reply-to field:    https://893639c5.ngrok.io/weather/api
        url += "/led/0/"
        if state == 2:
            code = "0"
            setting = 0
        else: # should be 3
            code = "1"
            setting = 1
        url += code
        data = { "warning": typestr }
        r = requests.post(url, json=data)
        if r.status_code == 200:
            print("Successfully set the alarm on reference device to %s." % code)
            alarm = setting
            #reply = r.json()
            #if reply.status < 0:
            #    print("Sent alarm %s with error reply: %s", (code, reply.message))
            #else:
            #    print("Successfully set the alarm on reference device to %s." % code)
            #    alarm = setting
        else:
            print("Unable to set alarm on reference device to %s due to error." % code)

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
            values = sqlContext.sql("select guid, payload.data.temperature as temperature, payload.data.pressure as pressure, payload.data.humidity as humidity, payload.data.ambient_light as ambient_light from iotmsgsTable")
            values.show(n=100)

            # Process statistics
            #tempValues.foreach(lambda x: add_temp(x.temperature))
            valuesList = values.collect()
            for x in valuesList:
                procstats(x)
            tempMean = get_temp_mean()
            tempStd = get_temp_stddev()
            print("Temperature - mean=", tempMean, ", std.dev=", tempStd)
            tempThreshLow = tempMean - tempStd * 2.0
            tempThreshHigh = tempMean + tempStd * 2.0
            print("Two-sigma Low temp is <", tempThreshLow, "and high temp is >", tempThreshHigh)
            #pressValues.foreach(lambda x: add_press(x.pressure))
            pressMean = get_press_mean()
            pressStd = get_press_stddev()
            print("Barometric Pressure - mean=", pressMean, ", std.dev=", pressStd)
            pressThreshLow = pressMean - pressStd * 2.0
            pressThreshHigh = pressMean + pressStd * 2.0
            print("Two-sigma Low pressure is <", pressThreshLow, "and high pressure is >", pressThreshHigh)
            #humidValues.foreach(lambda x: add_humid(x.humidity))
            humidMean = get_humid_mean()
            humidStd = get_humid_stddev()
            print("Humidity - mean=", humidMean, ", std.dev=", humidStd)
            humidThreshLow = humidMean - humidStd * 2.0
            humidThreshHigh = humidMean + humidStd * 2.0
            print("Two-sigma Low humid is <", humidThreshLow, "and high humid is >", humidThreshHigh)
            #lightValues.foreach(lambda x: add_light(x.ambient_light))
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
            for x in valuesList:
                #print("Device", x.guid, ", temperature", x.temperature)
                showerr(x.guid, "TEMPERATURE", x.temperature, tempThreshLow, tempThreshHigh)
                showerr(x.guid, "PRESSURE", x.pressure, pressThreshLow, pressThreshHigh)
                showerr(x.guid, "HUMIDITY", x.humidity, humidThreshLow, humidThreshHigh)
                showerr(x.guid, "LIGHT", x.ambient_light, lightThreshLow, lightThreshHigh)
            trigger_alarm()
            
            # Clean-up
	    sqlContext.dropTempTable("iotmsgsTable")
        # Catch any exceptions
        except KeyboardInterrupt:
            print("Persisting stats data 3...") # prob.needs transaction semantics to avoid partial saves tho
        except Exception as e:
            if str(e) != "Can not reduce() empty RDD":
                raise
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
