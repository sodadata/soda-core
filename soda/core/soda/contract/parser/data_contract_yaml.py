from __future__ import annotations

from numbers import Number
from typing import Dict, List

from ruamel.yaml import CommentedMap, CommentedSeq

from soda.contract.parser.data_contract_parser_logger import DataContractParserLogger


class YamlLocation:

    def __init__(self, file_path: str, line: int, column: int):
        self.file_path: str = file_path
        self.line: int = line
        self.column: int = column

    def __str__(self):
        return f'{self.file_path}[{self.line},{self.column}]'


class YamlValue:
    """
    Base class for yaml data structure objects
    """

    def __init__(self, ruamel_value: object, location: YamlLocation):
        self.location = location
        self.ruamel_value = ruamel_value

    def _convert_value(self, ruamel_value: object, line: int, column: int, logs: DataContractParserLogger) -> YamlValue:
        location = YamlLocation(file_path=self.location.file_path, line=line, column=column)
        if isinstance(ruamel_value, str):
            return YamlString(ruamel_value=ruamel_value, location=location)
        if isinstance(ruamel_value, bool):
            return YamlBoolean(value=ruamel_value, location=location)
        if isinstance(ruamel_value, Number):
            return YamlNumber(value=ruamel_value, location=location)
        if ruamel_value is None:
            return YamlNull(location=location)
        if isinstance(ruamel_value, CommentedMap):
            return YamlObject(ruamel_value=ruamel_value, location=location, logs=logs)
        if isinstance(ruamel_value, CommentedSeq):
            return YamlList(ruamel_value=ruamel_value, location=location, logs=logs)
        logs.error(f"Unsupported Ruamel YAML object type: {type(ruamel_value).__name__}\n{str(ruamel_value)}")


class YamlString(YamlValue):
    def __init__(self, ruamel_value: str, location: YamlLocation):
        super().__init__(ruamel_value, location)
        self.value: str = ruamel_value


class YamlNumber(YamlValue):
    def __init__(self, ruamel_value: Number, location: YamlLocation):
        super().__init__(ruamel_value, location)
        self.value: Number = ruamel_value


class YamlBoolean(YamlValue):
    def __init__(self, ruamel_value: bool, location: YamlLocation):
        super().__init__(ruamel_value, location)
        self.value: bool = ruamel_value


class YamlNull(YamlValue):
    def __init__(self, location: YamlLocation):
        super().__init__(None, location)
        self.value: None = None


class YamlObject(YamlValue):

    def __init__(self, ruamel_value: CommentedMap, location: YamlLocation, logs: DataContractParserLogger):
        super().__init__(ruamel_value, location)
        self.yaml_dict: Dict[str, YamlValue] = {
            key: self.__convert_map_value(ruamel_object=ruamel_value, key=key, value=value, logs=logs)
            for key, value in ruamel_value.items()
        }

    def __iter__(self):
        return list(self.yaml_dict.keys())

    def __convert_map_value(self, ruamel_object: CommentedMap, key, value, logs: DataContractParserLogger) -> YamlValue:
        ruamel_location = ruamel_object.lc.value(key)
        line: int = ruamel_location[0]
        column: int = ruamel_location[1]
        return self._convert_value(ruamel_value=value, line=line, column=column, logs=logs)

    def __iter__(self):
        return iter(self.yaml_dict)

    def read_object(self, key: str, logs: DataContractParserLogger) -> YamlObject | None:
        """
        An appropriate error log is generated if the value is not a YamlObject or if the key is missing
        :return: a YamlObject if the value for the key is a YamlObject, otherwise None.
        """
        return self.read_value(logs=logs, key=key, expected_type=YamlObject, required=True, default_value=None)

    def read_object_opt(self, key: str, logs: DataContractParserLogger) -> YamlObject | None:
        """
        An appropriate error log is generated if the value is not a YamlObject
        :return: a YamlObject if the value for the key is a YamlObject, otherwise None.
        """
        return self.read_value(logs=logs, key=key, expected_type=YamlObject, required=False, default_value=None)

    def read_list_opt(self, key: str, logs: DataContractParserLogger) -> YamlList | None:
        """
        An appropriate error log is generated if the value is not a YamlObject
        :return: a YamlObject if the value for the key is a YamlObject, otherwise None.
        """
        return self.read_value(logs=logs, key=key, expected_type=YamlList, required=False, default_value=None)

    def read_string_opt(self, key: str, logs: DataContractParserLogger, default_value: str | None = None) -> YamlString | None:
        """
        An appropriate error log is generated if the value is not a string
        :return: a YamlString if the value for the key is a YAML string, otherwise None.
        """
        return self.read_value(
            logs=logs, key=key, expected_type=YamlString, required=False, default_value=default_value
        )

    def read_string(self, key: str, logs: DataContractParserLogger) -> YamlString | None:
        """
        An appropriate error log is generated if the value is not a string or if the key is missing.
        :return: a YamlString if the value for the key is a YAML string, otherwise None.
        """
        return self.read_value(logs=logs, key=key, expected_type=YamlString, required=True)

    def read_value(self,
                   logs: DataContractParserLogger,
                   key: str,
                   expected_type: type = None,
                   required: bool = False,
                   default_value=None,
                   ) -> YamlValue:
        if key not in self.yaml_dict:
            if required:
                logs.error(f"'{key}' is required")
            return default_value
        yaml_value: YamlValue = self.yaml_dict.get(key)
        if not isinstance(yaml_value, expected_type):
            logs.error(
                message=f"'{key}' expected a {expected_type.__name__}, but was {type(yaml_value).__name__}",
                location=yaml_value.location,
            )
        return yaml_value

    def actual_type_name(self) -> str:
        return "object"


class YamlList(YamlValue):

    def __init__(self, ruamel_value: CommentedSeq, location: YamlLocation, logs: DataContractParserLogger):
        super().__init__(ruamel_value, location)
        self.value: List[YamlValue] = [
            self.__convert_array_value(ruamel_value=ruamel_list_value, commented_seq=ruamel_value, index=index, logs=logs)
            for index, ruamel_list_value in enumerate(ruamel_value)
        ]

    def __iter__(self):
        return iter(self.value)

    def __convert_array_value(self, ruamel_value, commented_seq: CommentedSeq, index: int, logs: DataContractParserLogger) -> YamlValue:
        ruamel_location = commented_seq.lc.key(index)
        line: int = ruamel_location[0]
        column: int = ruamel_location[1]
        return self._convert_value(ruamel_value=ruamel_value, line=line, column=column, logs=logs)
