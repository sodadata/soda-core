from __future__ import annotations

from numbers import Number

from ruamel.yaml import CommentedMap

from soda.contracts.impl.logs import Logs
from soda.contracts.impl.yaml import YamlLocation


class YamlHelper:

    def __init__(self, logs: Logs):
        self.logs: Logs = logs

    def read_yaml_object(self, d: dict, key: str) -> dict | None:
        """
        An error is generated if the value is missing or not a YAML object.
        :return: a dict if the value for the key is a YAML object, otherwise None.
        """
        return self.read_value(d=d, key=key, expected_type=dict, required=True, default_value=None)

    def read_yaml_object_opt(self, d: dict, key: str) -> dict | None:
        """
        An error is generated if the value is present and not a YAML object.
        :return: a dict if the value for the key is a YAML object, otherwise None.
        """
        return self.read_value(d=d, key=key, expected_type=dict, required=False, default_value=None)

    def read_yaml_list(self, d: dict, key: str) -> list | None:
        """
        An error is generated if the value is missing or not a YAML list.
        :return: a list if the value for the key is a YAML list, otherwise None.
        """
        return self.read_value(d=d, key=key, expected_type=list, required=True, default_value=None)

    def read_yaml_list_opt(self, d: dict, key: str) -> list | None:
        """
        An error is generated if the value is present and not a YAML list.
        :return: a list if the value for the key is a YAML list, otherwise None.
        """
        return self.read_value(d=d, key=key, expected_type=list, required=False, default_value=None)

    def read_string(self, d: dict, key: str) -> str | None:
        """
        An error is generated if the value is missing or not a string.
        :return: a str if the value for the key is a YAML string, otherwise None.
        """
        return self.read_value(d=d, key=key, expected_type=str, required=True, default_value=None)

    def read_string_opt(self, d: dict, key: str, default_value: str | None = None) -> str | None:
        """
        An error is generated if the value is present and not a string.
        :return: a str if the value for the key is a string, otherwise None.
        """
        return self.read_value(d=d, key=key, expected_type=str, required=False, default_value=default_value)

    def read_list_of_strings(self, d: dict, key: str) -> list[str] | None:
        list_value = self.read_value(d=d, key=key, expected_type=list, required=True, default_value=None)
        if isinstance(list_value, list):
            if all(isinstance(e, str) for e in list_value):
                return list_value
            else:
                location: YamlLocation | None = self.create_dict_value_location(d, key)
                self.logs.error(message=f"Not all elements in list '{key}' are strings", location=location)

    def create_dict_value_location(self, d: dict, key) -> YamlLocation | None:
        if isinstance(d, CommentedMap):
            ruamel_location = d.lc.value(key)
            line: int = ruamel_location[0]
            column: int = ruamel_location[1]
            return YamlLocation(line=line, column=column)
        return None

    def read_bool(self, d: dict, key: str) -> bool | None:
        """
        An appropriate error log is generated if the value is not a bool or if the key is missing
        :return: a bool if the value for the key is a YAML boolean, otherwise None.
        """
        return self.read_value(d=d, key=key, expected_type=bool, required=True, default_value=None)

    def read_bool_opt(self, d: dict, key: str, default_value: bool | None = None) -> bool | None:
        """
        An appropriate error log is generated if the value is not a bool.
        :return: a bool if the value for the key is a YAML boolean, otherwise None.
        """
        return self.read_value(d=d, key=key, expected_type=bool, required=False, default_value=default_value)

    def read_number(self, d: dict, key: str) -> Number | None:
        """
        An appropriate error log is generated if the value is not a number or if the key is missing
        :return: a bool if the value for the key is a YAML number, otherwise None.
        """
        return self.read_value(d=d, key=key, expected_type=Number, required=True, default_value=None)

    def read_number_opt(self, d: dict, key: str, default_value: Number | None = None) -> Number | None:
        """
        An appropriate error log is generated if the value is not a number.
        :return: a Number if the value for the key is a YAML number, otherwise None.
        """
        return self.read_value(d=d, key=key, expected_type=Number, required=False, default_value=default_value)

    def read_value(
        self,
        d: dict,
        key: str,
        expected_type: type = None,
        required: bool = False,
        default_value=None,
    ) -> object | None:
        if key not in d:
            if required:
                location = self.create_dict_value_location(d, key)
                self.logs.error(message=f"'{key}' is required", location=location)
            return default_value
        value = d.get(key)
        if expected_type is not None and not isinstance(value, expected_type):
            location = self.create_dict_value_location(d, key)
            self.logs.error(
                message=f"'{key}' expected a {expected_type.__name__}, but was {type(value).__name__}",
                location=location
            )
        return value
