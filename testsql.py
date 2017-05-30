# Header imports
from __future__ import print_function
from operator import add
from pyspark import SparkContext, SparkConf 

import sys
import json
import re

conf = SparkConf()
#conf.set("spark.ui.showConsoleProgress", "false")
sc = SparkContext(appName="PythonSparkScriptExample", conf=conf) 

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
from pyspark.sql.functions import mean, min, max

#----------- Your Code Below ----------- 

# Inject JSON to a JSON RDD
jsonRDD = sc.wholeTextFiles("iotmsgs.txt").map(lambda (k,v): v)
js = jsonRDD.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE))

iotmsgsRDD = sqlContext.read.json(js)
iotmsgsRDD.registerTempTable("iotmsgsTable")

print("JSON converted to DataFrame of casted floating point numbers")
sqlContext.sql("select distinct cast(payload.data.temperature as float) \
  from iotmsgsTable order by temperature desc").show()

print("DataFrame showing automated 'describe' summary of floating points")
sqlContext.sql("select distinct cast(payload.data.temperature as float) \
  from iotmsgsTable order by temperature desc").describe().show()

print("DataFrame of selected SQL dataframe functions")
temperatureDF = sqlContext.sql("select distinct cast(payload.data.temperature \
  as float) from iotmsgsTable order by temperature desc")
functionsDF = temperatureDF.select([mean('temperature'), min('temperature'), \
  max('temperature')])
print(type(functionsDF))
print(functionsDF)
functionsDF.printSchema()
functionsDF.show()

# Collect a List of Rows of data from the DataFrame
print("Extracted List of Rows of selected SQL dataframe function")
functionsList = temperatureDF.select([mean('temperature'), min('temperature'), \
  max('temperature')]).collect()
print(type(functionsList))
print(functionsList)
print()

# Collect max temperature from Row #1 of the DataFrame
print("Extracted value from cell of selected SQL dataframe function")
maxTemp = functionsList[0]['max(temperature)']
print(type(maxTemp))
print(maxTemp)
if (maxTemp > 95.0):
  print('*ALERT!* - Temperature rose above 95.0!') 
print()

# Collect List, then extract first Row from the DataFrame running functions:
#  mean, min, and max
print("Extracted value of iterated Row from selected DataFrame function")
summaryList = temperatureDF.select([mean('temperature'), min('temperature'), \
  max('temperature')]).collect()
print(type(summaryList))
print(summaryList)
summaryIter = iter(summaryList)
summaryRow = next(summaryIter)
avgTemp = summaryRow['avg(temperature)']
print(type(avgTemp))
print(avgTemp)
if (avgTemp > 95.0):
  print('Avg. temperature was above 95.0!') 
else:
  print('Avg. temperature was OK, not above 95.0') 

