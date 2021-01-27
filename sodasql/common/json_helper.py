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
        raise RuntimeError(f"Don't know how to jsonize {o} ({type(o)})")
