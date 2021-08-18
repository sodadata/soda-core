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
import json
from decimal import Decimal


class JsonHelper:

    @staticmethod
    def to_json(o):
        return json.dumps(o)

    @staticmethod
    def to_json_pretty(o):
        return json.dumps(o, indent=2)

    @staticmethod
    def to_jsonnable(o):
        if o is None \
            or isinstance(o, str) \
            or isinstance(o, int) \
            or isinstance(o, float) \
            or isinstance(o, bool):
            return o
        if isinstance(o, dict):
            for key, value in o.items():
                update = False
                if not isinstance(key, str):
                    del o[key]
                    key = str(key)
                    update = True
                jsonnable_value = JsonHelper.to_jsonnable(value)
                if value is not jsonnable_value:
                    value = jsonnable_value
                    update = True
                if update:
                    o[key] = value
            return o
        if isinstance(o, list):
            for i in range(len(o)):
                element = o[i]
                jsonnable_element = JsonHelper.to_jsonnable(element)
                if element is not jsonnable_element:
                    o[i] = jsonnable_element
            return o
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        if isinstance(o, datetime.date):
            return o.strftime('%Y-%m-%d')
        if isinstance(o, datetime.time):
            return o.strftime('%H:%M:%S')
        raise RuntimeError(f"Don't know how to jsonize {o} ({type(o)})")
