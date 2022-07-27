#!/usr/bin/env python

# standard libraries
from __future__ import print_function
import config
import json
from csv import DictReader
from datetime import datetime
import logging
from random import randrange
from time import sleep

# kafka-python libraries
from kafka import KafkaProducer
from kafka.errors import KafkaError


def run_kafka_broker(topic):
    """sends a new datum every x seconds to kafka
    
    """
    log_file = config.log_file
    topic = topic
    key = config.key
    csv_file = config.csv_file

    print("CSV file name = ",csv_file)

    logging.basicConfig(filename=log_file,level=config.log_level)
    logging.info("logging to " + config.kafka_host)
    mykafkaservers = [config.kafka_host]
    producer = KafkaProducer(
        bootstrap_servers=mykafkaservers, 
        value_serializer=lambda m: m.encode('utf-8'),
        key_serializer=str.encode,
        api_version=(0,10,1)
    )

    produce = lambda vals: producer.send(topic, value=vals, key=key+str(datetime.now()).replace(' ', ':'))
    while True:
        fh = DictReader(open(csv_file))
        for row in fh:
            line = json.dumps(row)
            print("LINE = ",line);
            logging.info("line# " + "\n line: " + line)
            produce(line)
            sleep(config.timeout)


if __name__ == "__main__":
    from sys import argv
    print(argv[1])
    run_kafka_broker(argv[1])
