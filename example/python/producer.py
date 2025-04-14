"""
This script is a simple sample generator for Kafka.
The provided Kafka Datasource visualizes those samples in Grafana.
"""

from confluent_kafka import Producer
from time import sleep
from random import random
import json

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

counter = 1

while True:
    data = {"value1": random(),
            "value2": 1 + random()}
    producer.produce("test", value=json.dumps(data))
    print("Sample #{} produced!".format(counter))
    counter += 1
    producer.flush(1)
    sleep(0.5)
