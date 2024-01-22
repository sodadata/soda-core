from __future__ import annotations

import logging
from numbers import Number
from typing import Any

from ruamel.yaml import YAML, CommentedMap, CommentedSeq, round_trip_dump
from ruamel.yaml.error import MarkedYAMLError

from soda.contracts.impl.logs import Logs

logger = logging.getLogger(__name__)


class YamlParser:

    def __init__(self, logs: Logs | None = None):
        self.ruamel_yaml: YAML = YAML()
        self.ruamel_yaml.preserve_quotes = True
        self.logs: Logs = logs if logs else Logs()

    def parse_yaml_str(self, yaml_str: str) -> object:
        """
        Parses the YAML string using the ruamel parser.  Returns a data structure representing
        the YAML with dicts (ruamel CommentedMap) and array (ruamel CommentedSeq) amd basic
        python types.
        Swallows Ruamel parsing exceptions and logs them either to the self.logs object
        """
        try:
            return self.ruamel_yaml.load(yaml_str)
        except MarkedYAMLError as e:
            location = self.get_location_from_yaml_error(e)
            msg = f"YAML syntax error at {location}: {e}"
            self.logs.error(message=msg, exception=e)

    @classmethod
    def get_location_from_yaml_error(cls, e):
        mark = e.context_mark if e.context_mark else e.problem_mark
        return YamlLocation(
            line=mark.line + 1,
            column=mark.column + 1,
        )


class YamlWriter:
    def __init__(self, logs: Logs | None = None):
        self.logs: Logs = logs if logs else Logs()

    def write_to_yaml_str(self, yaml_object: object) -> str:
        try:
            return round_trip_dump(yaml_object)
        except Exception as e:
            self.logs.error(f"Couldn't write SodaCL YAML object: {e}", exception=e)


class YamlWrapper:

    """
    Wraps Ruamel YAML objects into YamlValue's that provide
     - capturing all errors into a central logs object instead of raising an exception on the first problem
     - convenience read_* methods on the YamlObject for writing parsing code
     - a more convenient way to access the line and column information (location)
    """

    def __init__(self, logs: Logs = Logs()):
        self.logs: Logs = logs

    def wrap(self, ruamel_value: object) -> YamlValue:
        if isinstance(ruamel_value, str):
            return YamlString(ruamel_value=ruamel_value)
        if isinstance(ruamel_value, bool):
            return YamlBoolean(ruamel_value=ruamel_value)
        if isinstance(ruamel_value, Number):
            return YamlNumber(ruamel_value=ruamel_value)
        if ruamel_value is None:
            return YamlNull()
        if isinstance(ruamel_value, CommentedMap):
            return YamlObject(ruamel_value=ruamel_value, yaml_wrapper=self)
        if isinstance(ruamel_value, CommentedSeq):
            return YamlList(ruamel_value=ruamel_value, yaml_wrapper=self)
        logging.error(f"Unsupported Ruamel YAML object type: {type(ruamel_value).__name__}\n{str(ruamel_value)}")


class YamlLocation:
    def __init__(self, line: int, column: int):
        self.line: int = line
        self.column: int = column

    def __str__(self):
        return f"(line {self.line},col {self.column})"


class YamlValue:
    """
    Base class for yaml data structure objects
    """

    def __init__(self, ruamel_value: object):
        self.ruamel_value: Any = ruamel_value
        self.location: YamlLocation | None = None

    def set_location(self, location: YamlLocation | None) -> None:
        self.location = location

    def unpacked(self) -> Any:
        return self.ruamel_value


class YamlString(YamlValue):
    def __init__(self, ruamel_value: str):
        super().__init__(ruamel_value)


class YamlNumber(YamlValue):
    def __init__(self, ruamel_value: Number):
        super().__init__(ruamel_value)


class YamlBoolean(YamlValue):
    def __init__(self, ruamel_value: bool):
        super().__init__(ruamel_value)


class YamlNull(YamlValue):
    def __init__(self):
        super().__init__(None)


class YamlObject(YamlValue):
    def __init__(self, ruamel_value: CommentedMap, yaml_wrapper: YamlWrapper):
        super().__init__(ruamel_value)
        self.logs: Logs = yaml_wrapper.logs
        self.yaml_dict: dict[str, YamlValue] = {
            key: self.__convert_map_value(ruamel_object=ruamel_value, key=key, value=value, yaml_wrapper=yaml_wrapper)
            for key, value in ruamel_value.items()
        }

    def __convert_map_value(self, ruamel_object: CommentedMap, key, value, yaml_wrapper: YamlWrapper) -> YamlValue:
        ruamel_location = ruamel_object.lc.value(key)
        line: int = ruamel_location[0]
        column: int = ruamel_location[1]
        yaml_value = yaml_wrapper.wrap(ruamel_value=value)
        yaml_value.set_location(YamlLocation(line=line, column=column))
        return yaml_value

    def __iter__(self):
        return iter(self.yaml_dict)

    def __contains__(self, key):
        return key in self.yaml_dict

    def __len__(self) -> int:
        return len(self.yaml_dict)

    def keys(self):
        return self.yaml_dict.keys()

    def items(self):
        return self.yaml_dict.items()

    def get(self, index) -> YamlValue | None:
        return self.yaml_dict.get(index)

    def read_yaml_object(self, key: str) -> YamlObject | None:
        """
        An appropriate error log is generated if the value is not a YamlObject or if the key is missing
        :return: a YamlObject if the value for the key is a YamlObject, otherwise None.
        """
        return self.read_value(key=key, expected_type=YamlObject, required=True, default_value=None)

    def read_yaml_object_opt(self, key: str) -> YamlObject | None:
        """
        An appropriate error log is generated if the value is not a YamlObject
        :return: a YamlObject if the value for the key is a YamlObject, otherwise None.
        """
        return self.read_value(key=key, expected_type=YamlObject, required=False, default_value=None)

    def read_yaml_list(self, key: str) -> YamlList | None:
        """
        An appropriate error log is generated if the value is not a YamlList or if the key is missing
        :return: a YamlList if the value for the key is a YamlList, otherwise None.
        """
        return self.read_value(key=key, expected_type=YamlList, required=True, default_value=None)

    def read_yaml_list_opt(self, key: str) -> YamlList | None:
        """
        An appropriate error log is generated if the value is not a YamlObject
        :return: a YamlObject if the value for the key is a YamlObject, otherwise None.
        """
        return self.read_value(key=key, expected_type=YamlList, required=False, default_value=None)

    def read_yaml_string_opt(self, key: str, default_value: str | None = None) -> YamlString | None:
        """
        An appropriate error log is generated if the value is not a string
        :return: a YamlString if the value for the key is a YAML string, otherwise None.
        """
        return self.read_value(key=key, expected_type=YamlString, required=False, default_value=default_value)

    def read_yaml_string(self, key: str) -> YamlString | None:
        """
        An appropriate error log is generated if the value is not a string or if the key is missing.
        :return: a YamlString if the value for the key is a YAML string, otherwise None.
        """
        return self.read_value(key=key, expected_type=YamlString, required=True)

    def read_string_opt(self, key: str, default_value: str | None = None) -> str | None:
        """
        An appropriate error log is generated if the value is not a string
        :return: a str if the value for the key is a YAML string, otherwise None.
        """
        yaml_string_value = self.read_value(
            key=key, expected_type=YamlString, required=False, default_value=default_value
        )
        return yaml_string_value.unpacked() if isinstance(yaml_string_value, YamlString) else None

    def read_string(self, key: str) -> str | None:
        """
        An appropriate error log is generated if the value is not a string or if the key is missing.
        :return: a YamlString if the value for the key is a YAML string, otherwise None.
        """
        yaml_string_value = self.read_value(key=key, expected_type=YamlString, required=True, default_value=None)
        return yaml_string_value.unpacked() if isinstance(yaml_string_value, YamlString) else None

    def read_list_strings(self, key: str) -> list[str] | None:
        yaml_list_value = self.read_value(key=key, expected_type=YamlList, required=True, default_value=None)
        if isinstance(yaml_list_value, YamlList):
            value = yaml_list_value.unpacked()
            if all(isinstance(e, str) for e in value):
                return value
            else:
                self.logs.error(f"Not all elements in list are strings: {yaml_list_value.location}")

    def read_bool(self, key: str) -> bool | None:
        """
        An appropriate error log is generated if the value is not a bool or if the key is missing
        :return: a bool if the value for the key is a YAML boolean, otherwise None.
        """
        yaml_boolean_value = self.read_value(key=key, expected_type=YamlBoolean, required=True, default_value=None)
        return yaml_boolean_value.unpacked() if isinstance(yaml_boolean_value, YamlString) else None

    def read_bool_opt(self, key: str, default_value: bool | None = None) -> bool | None:
        """
        An appropriate error log is generated if the value is not a bool.
        :return: a bool if the value for the key is a YAML boolean, otherwise None.
        """
        yaml_boolean_value = self.read_value(
            key=key, expected_type=YamlBoolean, required=False, default_value=default_value
        )
        return yaml_boolean_value.unpacked() if isinstance(yaml_boolean_value, YamlBoolean) else None

    def read_number(self, key: str) -> Number | None:
        """
        An appropriate error log is generated if the value is not a number or if the key is missing
        :return: a bool if the value for the key is a YAML number, otherwise None.
        """
        yaml_number_value = self.read_value(key=key, expected_type=YamlNumber, required=True, default_value=None)
        return yaml_number_value.unpacked() if isinstance(yaml_number_value, YamlNumber) else None

    def read_number_opt(self, key: str, default_value: Number | None = None) -> Number | None:
        """
        An appropriate error log is generated if the value is not a number.
        :return: a Number if the value for the key is a YAML number, otherwise None.
        """
        yaml_number_value = self.read_value(
            key=key, expected_type=YamlNumber, required=False, default_value=default_value
        )
        return yaml_number_value.unpacked() if isinstance(yaml_number_value, YamlNumber) else None

    def read_value(
        self,
        key: str,
        expected_type: type = None,
        required: bool = False,
        default_value=None,
    ) -> YamlValue:
        if key not in self.yaml_dict:
            if required:
                msg = f"'{key}' is required {self.location}"
                if self.logs:
                    self.logs.error(msg)
                else:
                    logger.error(msg)
            return default_value
        yaml_value: YamlValue = self.yaml_dict.get(key)
        if expected_type is not None and not isinstance(yaml_value, expected_type):
            msg = f"'{key}' expected a {expected_type.__name__}, but was {type(yaml_value).__name__} {yaml_value.location}"
            if self.logs:
                self.logs.error(msg)
            else:
                logger.error(msg)
        return yaml_value

class YamlList(YamlValue):
    def __init__(self, ruamel_value: CommentedSeq, yaml_wrapper: YamlWrapper):
        super().__init__(ruamel_value)
        self.value: list[YamlValue] = [
            self.__convert_array_value(
                ruamel_value=ruamel_list_value,
                commented_seq=ruamel_value,
                index=index,
                yaml_wrapper=yaml_wrapper
            )
            for index, ruamel_list_value in enumerate(ruamel_value)
        ]

    def __convert_array_value(self,
                              ruamel_value,
                              commented_seq: CommentedSeq,
                              index: int,
                              yaml_wrapper: YamlWrapper
                              ) -> YamlValue:
        ruamel_location = commented_seq.lc.key(index)
        line: int = ruamel_location[0]
        column: int = ruamel_location[1]
        yaml_value = yaml_wrapper.wrap(ruamel_value=ruamel_value)
        yaml_value.set_location(YamlLocation(line=line, column=column))
        return yaml_value

    def __iter__(self):
        return iter(self.value)

    def __contains__(self, key):
        return key in self.value

    def __len__(self) -> int:
        return len(self.value)

    def read_yaml_objects(self) -> list[YamlObject]:
        return [e for e in self.value if isinstance(e, YamlObject)]
