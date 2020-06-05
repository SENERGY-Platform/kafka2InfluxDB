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

import unittest

import lib
import json


class TestMainMethods(unittest.TestCase):

    def test_get_fields_values(self):
        field_conf = json.loads('{ "isOn:bool": "analytics.isOn", "timestamp:string": "analytics.timestamp"}')
        with open('test/data/test.json') as json_file:
            data_in = json.load(json_file)
        print(json.dumps(lib.get_field_values(field_conf, data_in)))
        self.assertEqual('{"isOn": false, "timestamp": "2020-06-04T11:17:37.000Z"}'
                         , json.dumps(lib.get_field_values(field_conf, data_in)))


