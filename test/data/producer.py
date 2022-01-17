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

import json
import datetime as dt

from confluent_kafka import Producer

conf = {'bootstrap.servers': "localhost:9092",
        'client.id': "Test-Producer"}

producer = Producer(conf)

topic = "topic"


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


with open('test.json') as json_file:
    data = json.load(json_file)

for n in range(10):
        record_key = "test"
        time = dt.datetime.utcnow() - dt.timedelta(seconds=n)
        data["time"] = time.strftime("%Y%m%dT%H%M%S.%fZ")
        record_value = json.dumps(data)
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        producer.poll(0)
producer.flush()
