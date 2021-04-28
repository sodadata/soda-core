#  Copyright 2020 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import datetime
from unittest import TestCase

from sodasql.common.json_helper import JsonHelper


class TestJsonHelper(TestCase):

    def test_jsonize_date(self):
        self.assertEqual(JsonHelper.to_jsonnable(datetime.date(2021, 1, 2)), '2021-01-02')

    def test_jsonize_datetime(self):
        self.assertEqual(JsonHelper.to_jsonnable(datetime.datetime(2021, 1, 2, 10, 5, 23)), '2021-01-02T10:05:23')

    def test_jsonize_time(self):
        self.assertEqual(JsonHelper.to_jsonnable(datetime.time(10, 5, 23)), '10:05:23')
