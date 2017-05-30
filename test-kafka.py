'''
Simplest Kafka Producer Test Script
'''
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError

#default_server = 'ec2-54-183-62-84.us-west-1.compute.amazonaws.com:9092'
default_server = 'localhost:9092'
#default_retries = 3
topic = 'iotmsgs'  #"x42ws.public.data"

producer = KafkaProducer(bootstrap_servers=[default_server])
producer.send(topic, b'X42') # returns an async future
producer.flush() # block until all async messages are sent
print "Made it!"

