#  Copyright 2020 InfAI (CC SES)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from objectpath import *
import json
from influxdb import exceptions
from confluent_kafka import KafkaError


def get_field_values(field_conf, data_in):
    fields = {}
    for (key, value) in field_conf.items():
        try:
            key_conf = key.split(":")
            key = key_conf[0]
            key_type = key_conf[1]
            val = Tree(data_in).execute('$.' + value)
            if type(val) == key_type:
                fields[key] = val
            else:
                if key_type == "string":
                    fields[key] = str(val)
                elif key_type == "float":
                    fields[key] = float(val)
                elif key_type == "int":
                    fields[key] = int(val)
        except Exception as e:
            print('Could not parse value for key ' + key)
            print(e)
            if key_type == "string":
                fields[key] = ''
            elif key_type == "float":
                fields[key] = 0.0
            elif key_type == "int":
                fields[key] = 0
    return fields


def start(client, c,DATA_FILTER_ID_MAPPING, DATA_FILTER_ID, DATA_MEASUREMENT, DATA_TIME_MAPPING, field_config):
    running = True
    while running:
        msg = c.poll()
        if not msg.error():
            print('Received message: %s' % msg.value().decode('utf-8'))
            data_input = json.loads(msg.value().decode('utf-8'))
            if Tree(data_input).execute('$.' + DATA_FILTER_ID_MAPPING) == DATA_FILTER_ID:
                body = {
                    "measurement": DATA_MEASUREMENT,
                    "time": Tree(data_input).execute('$.' + DATA_TIME_MAPPING),
                    "fields": get_field_values(field_config, data_input)
                }
                print(body)
                try:
                    client.write_points([body])
                except exceptions.InfluxDBClientError as e:
                    print(e.content)
        elif msg.error().code() != KafkaError.PARTITION_EOF:
            print(msg.error())
            running = False


