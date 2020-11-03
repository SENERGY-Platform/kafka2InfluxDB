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

DEBUG = os.getenv('DEBUG', "false")


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
                 time_precision=None):
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

    def start(self):
        print("starting export", flush=True)
        running = True
        try:
            self.consumer.subscribe([self.topic])
            while running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                    running = False
                else:
                    self.process_msg(msg)
        except Exception as e:
            print(e)
            sys.exit()
        finally:
            self.consumer.close()

    def process_msg(self, msg):
        if DEBUG == "true":
            print('Received message: %s' % msg.value().decode('utf-8'), flush=True)
        data_input = json.loads(msg.value().decode('utf-8'))
        if self.filter_msg(data_input):
            body = {
                "measurement": self.data_measurement,
                "fields": self.get_field_values(data_input)
            }
            if self.try_time:
                try:
                    body["time"] = Tree(data_input).execute('$.' + self.data_time_mapping)
                except SyntaxError as err:
                    print('Disabling reading time from message, error occurred:', err.msg, flush=True)
                    print('Influx will set time to time of arrival by default', flush=True)
                    self.try_time = False
            if DEBUG == "true":
                print('Write message: %s' % body, flush=True)
            try:
                self.influx_client.write_points([body], time_precision=self.time_precision)
            except exceptions.InfluxDBClientError as e:
                print(e.content)

    def get_field_values(self, data_in):
        fields = {}
        for (key, value) in self.field_config.items():
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
                    elif key_type == "bool":
                        fields[key] = bool(val)
            except Exception as e:
                print('Could not parse value for key ' + key)
                print(e)
                if key_type == "string":
                    fields[key] = ''
                elif key_type == "float":
                    fields[key] = 0.0
                elif key_type == "int":
                    fields[key] = 0
                elif key_type == "bool":
                    fields[key] = False
        return fields

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
