from __future__ import annotations

from numbers import Number

from ruamel.yaml import CommentedMap, CommentedSeq, round_trip_dump

from soda.contracts.impl.logs import Logs, Location


class YamlHelper:

    def __init__(self, logs: Logs):
        self.logs: Logs = logs

    def write_to_yaml_str(self, yaml_object: object) -> str:
        try:
            return round_trip_dump(yaml_object)
        except Exception as e:
            self.logs.error(f"Couldn't write SodaCL YAML object: {e}", exception=e)

    @classmethod
    def create_location_from_yaml_dict_key(cls, d: dict, key) -> Location | None:
        if isinstance(d, CommentedMap):
            ruamel_location = d.lc.value(key)
            line: int = ruamel_location[0]
            column: int = ruamel_location[1]
            return Location(line=line, column=column)
        return None

    @classmethod
    def create_location_from_yaml_value(cls, d: object) -> Location | None:
        if isinstance(d, CommentedMap) or isinstance(d, CommentedSeq):
            return Location(line=d.lc.line, column=d.lc.column)
        return None

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
                location: Location | None = self.create_location_from_yaml_dict_key(d, key)
                self.logs.error(message=f"Not all elements in list '{key}' are strings", location=location)

    def read_range(self, d: dict, key: str) -> "Range" | None:
        range_yaml: list | None = self.read_yaml_list_opt(d, key)
        if isinstance(range_yaml, list):
            if all(isinstance(range_value, Number) for range_value in range_yaml) and len(range_yaml) == 2:
                from soda.contracts.check import Range
                return Range(lower_bound=range_yaml[0], upper_bound=range_yaml[1])
            else:
                location: Location = self.create_location_from_yaml_value(range_yaml)
                self.logs.error("range expects a list of 2 numbers", location=location)

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
                location = self.create_location_from_yaml_value(d)
                self.logs.error(message=f"'{key}' is required", location=location)
            return default_value
        value = d.get(key)
        if expected_type is not None and not isinstance(value, expected_type):
            location = self.create_location_from_yaml_dict_key(d, key)
            self.logs.error(
                message=f"'{key}' expected a {expected_type.__name__}, but was {type(value).__name__}",
                location=location
            )
        return value
