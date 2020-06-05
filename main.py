#  Copyright 2018 InfAI (CC SES)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import json

import lib

import os

from confluent_kafka import Consumer
from influxdb import InfluxDBClient

if os.path.isfile('./.env'):
    from dotenv import load_dotenv
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    load_dotenv(dotenv_path)

INFLUX_HOST = os.getenv('INFLUX_HOST', "localhost")
INFLUX_PORT = os.getenv('INFLUX_PORT', 8086)
INFLUX_USER = os.getenv('INFLUX_USER', "root")
INFLUX_PW = os.getenv('INFLUX_PW', "")
INFLUX_DB = os.getenv('INFLUX_DB', "example")
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', "localhost:9092")
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', "topic")
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', "influx_reader")
DATA_MEASUREMENT = os.getenv('DATA_MEASUREMENT', "iot_device")
DATA_TIME_MAPPING = os.getenv('DATA_TIME_MAPPING', "time")

DATA_FILTER_TYPE = os.getenv('DATA_FILTER_TYPE', "deviceId")
DATA_FILTER_ID_MAPPING = os.getenv('DATA_FILTER_ID_MAPPING', "device_id")
DATA_FILTER_ID = os.getenv('DATA_FILTER_ID', "device_id")

DATA_FIELDS_MAPPING = os.getenv('DATA_FIELDS_MAPPING', '{"value:float": "value"}')

field_config = json.loads(DATA_FIELDS_MAPPING)

client = InfluxDBClient(INFLUX_HOST,
                        INFLUX_PORT,
                        INFLUX_USER,
                        INFLUX_PW,
                        INFLUX_DB)

client.create_database(INFLUX_DB)

c = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': KAFKA_GROUP_ID,
    'default.topic.config': {
        'auto.offset.reset': os.getenv('OFFSET_RESET', 'smallest')
    },
    'max.poll.interval.ms': 600000})

# c.assign(TopicPartition('0', 0))

c.subscribe([KAFKA_TOPIC])

lib.start(client, c, DATA_FILTER_ID_MAPPING, DATA_FILTER_ID, DATA_MEASUREMENT, DATA_TIME_MAPPING, field_config)

c.close()
