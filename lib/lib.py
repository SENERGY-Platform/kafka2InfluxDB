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
import os

from confluent_kafka.cimpl import KafkaException
from influxdb.client import InfluxDBClient
from objectpath import *
import json
from influxdb import exceptions
from confluent_kafka import KafkaError

# quick fix for objectpath module recognizing "-" as operator in field names
def get_value(data, path):
    path = path.split(".")
    value = data
    for part in path:
        value = value[part]
    return value


bool_true = {"True", "true", "1", 1}
bool_false = {"False", "false", "0", 0}


def to_bool(value):
    if value in bool_true:
        return True
    if value in bool_false:
        return False


def to_json_str(value):
    return json.dumps(value, separators=(',', ':'))


key_type_map = {
    "string": (str, str),
    "float": (float, float),
    "int": (int, int),
    "bool": (bool, to_bool),
    "string_json": (str, to_json_str)
}


def get_field_values(field_config, data_in):
    fields = {}
    for (key, value) in field_config.items():
        try:
            key_conf = key.split(":")
            key = key_conf[0]
            key_type = key_conf[1]
            # val = Tree(data_in).execute('$.' + value)
            val = get_value(data_in, value)
            if val is not None:
                if isinstance(val, key_type_map[key_type][0]):
                    fields[key] = val
                else:
                    fields[key] = key_type_map[key_type][1](val)
        except Exception as e:
            print('Could not parse value for key ' + key)
            print(e)
    return fields


class Kafka2Influx:

    def __init__(self,
                 consumer,
                 topic,
                 influx_client: InfluxDBClient,
                 data_filter_id_mapping,
                 data_filter_id,
                 data_measurement,
                 data_time_mapping,
                 field_config,
                 time_precision=None,
                 tag_config={},
                 debug=False):
        self.consumer = consumer
        self.topic = topic
        self.try_time = True
        self.data_filter_id_mapping = data_filter_id_mapping
        self.data_filter_id = data_filter_id
        self.data_measurement = data_measurement
        self.data_time_mapping = data_time_mapping
        self.field_config = field_config
        self.time_precision = time_precision
        self.influx_client = influx_client
        self.tag_config = tag_config
        self.debug = debug

    def start(self):
        print("starting export", flush=True)
        running = True
        try:
            self.consumer.subscribe([self.topic])
            while running:
                msgs = self.consumer.consume(num_messages=10000, timeout=1.0)
                if msgs is None or len(msgs) == 0:
                    continue
                else:
                    running = self.process_msgs(msgs)
        except Exception as e:
            print(e)
            sys.exit(1)
        finally:
            self.consumer.close()

    def process_msgs(self, msgs) -> bool:
        points = []
        for msg in msgs:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
                return False

            if self.debug:
                print('Received message: %s' % msg.value().decode('utf-8'), flush=True)
            data_input = json.loads(msg.value().decode('utf-8'))
            if self.filter_msg(data_input):
                body = {
                    "measurement": self.data_measurement,
                    "fields": get_field_values(self.field_config, data_input)
                }
                if self.try_time:
                    try:
                        body["time"] = Tree(data_input).execute('$.' + self.data_time_mapping)
                    except SyntaxError as err:
                        print('Disabling reading time from message, error occurred:', err.msg, flush=True)
                        print('Influx will set time to time of arrival by default', flush=True)
                        self.try_time = False
                if len(self.tag_config) > 0:
                    body["tags"] = get_field_values(self.tag_config, data_input)
                if self.debug:
                    print('Write message: %s' % body, flush=True)
                points.append(body)
        try:
            self.influx_client.write_points(points, time_precision=self.time_precision)
        except exceptions.InfluxDBClientError as e:
            print('Received Influx error: %s', e.content, flush=True)
        return True

    def filter_msg(self, data_input):
        if self.data_filter_id_mapping == 'device_id' or self.data_filter_id_mapping == 'import_id':
            if Tree(data_input).execute('$.' + self.data_filter_id_mapping) == self.data_filter_id:
                return True
        elif self.data_filter_id_mapping == 'operator_id':
            pipe_filter = self.data_filter_id.split(':')
            if (Tree(data_input).execute('$.pipeline_id') == pipe_filter[0]) & (
                    Tree(data_input).execute('$.operator_id') == pipe_filter[1]):
                return True
        return False
