from __future__ import annotations

import os
import re
from numbers import Number
from typing import Iterable

from ruamel.yaml import YAML, CommentedMap, CommentedSeq
from ruamel.yaml.error import MarkedYAMLError

from soda_core.common.logs import Logs, Location


class YamlParser:

    def __init__(self):
        self.ruamel_yaml_parser: YAML = YAML()
        self.ruamel_yaml_parser.preserve_quotes = True


class YamlFile:

    __yaml_parser = YamlParser()

    @classmethod
    def from_str(cls, yaml_str: str) -> YamlFile:
        return YamlFile(
            yaml_str=yaml_str
        )

    @classmethod
    def from_file_path(cls, yaml_file_path: str) -> YamlFile:
        return YamlFile(
            yaml_file_path=yaml_file_path
        )

    def __init__(
        self,
        yaml_file_path: str | None = None,
        yaml_str: str | None = None,
        yaml_dict: dict | None = None,
        logs: Logs | None = None,
        file_type: str | None = None
    ):
        self.file_path: str | None = yaml_file_path
        self.is_parsed: bool = False
        self.unresolved_yaml_str: str | None = None
        self.yaml_str: str | None = yaml_str
        self.yaml_dict: dict | None = yaml_dict
        self.logs: Logs = logs if isinstance(logs, Logs) else Logs()
        self.description: str = self.__create_description(file_type)
        # Use self.get_yaml_object() to ensure the file is parsed
        self._yaml_object: YamlObject | None = None

    def __create_description(self, file_type: str | None = None) -> str:
        file_type = f"{file_type} " if file_type else ""
        if self.file_path:
            return f"{file_type}YAML file '{self.file_path}'"
        if self.yaml_str and self.unresolved_yaml_str is None:
            return f"{file_type}provided YAML str"
        if self.yaml_str and self.unresolved_yaml_str:
            return f"{file_type}provided, resolved YAML str"
        if self.yaml_dict:
            return f"{file_type}provided dict"
        return f"no {file_type} YAML source provided"

    def __str__(self) -> str:
        return self.description

    def parse(self, variables: dict | None = None) -> YamlObject | None:
        self.is_parsed = True
        self.__read_yaml_file()
        self.__resolve_yaml_str(variables=variables)
        self.__parse_yaml_object()
        return self._yaml_object

    def has_yaml_object(self) -> bool:
        return isinstance(self._yaml_object, YamlObject)

    def __read_yaml_file(self) -> YamlFile:
        if isinstance(self.file_path, str) and self.yaml_str is None:
            try:
                with open(self.file_path) as f:
                    self.yaml_str = f.read()
            except OSError as e:
                if not os.path.exists(self.file_path):
                    self.logs.error(
                        message=f"{self.description} does not exist"
                    )
                elif not os.path.isdir(self.file_path):
                    self.logs.error(
                        message=f"{self.description} is a directory"
                    )
                else:
                    self.logs.error(
                        message=f"{self.description} can't be read",
                        exception=e
                    )
        return self

    def __resolve_yaml_str(self, variables: dict | None = None) -> YamlFile:
        if isinstance(self.yaml_str, str) and self.unresolved_yaml_str is None:
            self.unresolved_yaml_str = self.yaml_str
            variable_resolver = VariableResolver(variables=variables, logs=self.logs)
            self.yaml_str = variable_resolver.resolve(self.yaml_str)
        return self

    def __parse_yaml_object(self) -> None:
        if isinstance(self.yaml_str, str) and self.yaml_dict is None:
            try:
                self.yaml_dict = self.__yaml_parser.ruamel_yaml_parser.load(self.yaml_str)
            except MarkedYAMLError as e:
                mark = e.context_mark if e.context_mark else e.problem_mark
                line = mark.line + 1
                col = mark.column + 1
                location = Location(file_path=self.description, line=line, column=col)
                self.logs.error(message=f"YAML syntax error in {self.description}", exception=e, location=location)
                return
        if self.yaml_dict is not None and not isinstance(self.yaml_dict, dict):
            self.logs.error(
                message=f"Expected top level YAML object in {self.description}, "
                        f"but was {type(self.yaml_dict).__name__}"
            )
            self.yaml_dict = None
        if isinstance(self.yaml_dict, dict):
            self._yaml_object = YamlObject(
                yaml_file=self, yaml_dict=self.yaml_dict
            )

    def get_yaml_object(self) -> YamlObject:
        if not self.is_parsed:
            self.parse()
        return self._yaml_object

    def has_errors(self) -> bool:
        return self.logs.has_errors()

    def assert_no_errors(self) -> None:
        if self.logs.has_errors():
            raise AssertionError(str(self))


class YamlValue:

    def __init__(self, yaml_file: YamlFile) -> None:
        self.yaml_file: YamlFile = yaml_file
        self.logs: Logs = yaml_file.logs

    def _yaml_wrap(self, value):
        if isinstance(value, dict):
            return YamlObject(yaml_file=self.yaml_file, yaml_dict=value)
        if isinstance(value, list):
            return YamlList(yaml_file=self.yaml_file, yaml_list=value)
        return value

    @classmethod
    def yaml_unwrap(cls, o) -> object | None:
        if isinstance(o, YamlObject):
            return {k: cls.yaml_unwrap(v) for k, v in o.items()}
        if isinstance(o, YamlList):
            return [cls.yaml_unwrap(e) for e in o]
        return o


class YamlObject(YamlValue):

    def __init__(self, yaml_file: YamlFile, yaml_dict: dict) -> None:
        super().__init__(yaml_file)
        self.yaml_dict: dict = yaml_dict

    def items(self) -> list[tuple]:
        return [(k,self._yaml_wrap(v)) for k, v in self.yaml_dict]

    def __iter__(self) -> iter:
        return iter(self.yaml_dict.keys())

    def read_object(self, key: str) -> YamlObject | None:
        """
        An error is generated if the value is missing or not a YAML object.
        :return: a dict if the value for the key is a YAML object, otherwise None.
        """
        return self.read_value(key=key, expected_type=dict, required=True, default_value=None)

    def read_object_opt(self, key: str) -> YamlObject | None:
        """
        An error is generated if the value is present and not a YAML object.
        :return: a dict if the value for the key is a YAML object, otherwise None.
        """
        return self.read_value(key=key, expected_type=dict, required=False, default_value=None)

    def read_list(self, key: str, expected_element_type: type | None = None) -> YamlList | None:
        """
        An error is generated if the value is missing or not a YAML list.
        :return: a list if the value for the key is a YAML list, otherwise None.
        """
        list_value: YamlList | None = self.read_value(key=key, expected_type=list, required=True, default_value=None)
        if isinstance(list_value, YamlList):
            filtered_list: list = []
            if isinstance(expected_element_type, type):
                for index, element in enumerate(list_value.yaml_list):
                    if isinstance(element, expected_element_type):
                        filtered_list.append(element)
                    else:
                        self.yaml_file.logs.error(
                            message=f"'{key}' expects a list of {expected_element_type.__name__}. "
                                    f"But element {index} was {type(element).__name__}",
                            location=self.create_location_from_yaml_dict_key(key)
                        )
            list_value.yaml_list = filtered_list
        return list_value

    def read_list_opt(self, key: str) -> YamlList | None:
        """
        An error is generated if the value is present and not a YAML list.
        :return: a list if the value for the key is a YAML list, otherwise None.
        """
        return self.read_value(key=key, expected_type=list, required=False, default_value=None)

    def read_list_of_objects(self, key: str) -> YamlList | None:
        list_value: YamlList | None = self.read_list(key=key, expected_element_type=YamlObject)
        if isinstance(list_value, YamlList):
            for i in range(0, len(list_value.yaml_list)):
                element = list_value.yaml_list[i]
                if not isinstance(element, YamlObject):
                    self.yaml_file.logs.error(
                        message=f"'{key}' expected list of objects.  But element {i} was {type(element).__name__}",
                        location=self.create_location_from_yaml_dict_key(key)
                    )
                    list_value.yaml_list[i] = None
        return list_value

    def read_list_of_strings(self, key: str) -> list[str] | None:
        return self.read_list(key, expected_element_type=str)

    def read_string(self, key: str) -> str | None:
        """
        An error is generated if the value is missing or not a string.
        :return: a str if the value for the key is a YAML string, otherwise None.
        """
        return self.read_value(key=key, expected_type=str, required=True, default_value=None)

    def read_string_opt(self, key: str, default_value: str | None = None) -> str | None:
        """
        An error is generated if the value is present and not a string.
        :return: a str if the value for the key is a string, otherwise None.
        """
        return self.read_value(key=key, expected_type=str, required=False, default_value=default_value)

    def read_bool(self, key: str) -> bool | None:
        """
        An appropriate error log is generated if the value is not a bool or if the key is missing
        :return: a bool if the value for the key is a YAML boolean, otherwise None.
        """
        return self.read_value(key=key, expected_type=bool, required=True, default_value=None)

    def read_bool_opt(self, key: str, default_value: bool | None = None) -> bool | None:
        """
        An appropriate error log is generated if the value is not a bool.
        :return: a bool if the value for the key is a YAML boolean, otherwise None.
        """
        return self.read_value(key=key, expected_type=bool, required=False, default_value=default_value)

    def read_number(self, key: str) -> Number | None:
        """
        An appropriate error log is generated if the value is not a number or if the key is missing
        :return: a bool if the value for the key is a YAML number, otherwise None.
        """
        return self.read_value(key=key, expected_type=Number, required=True, default_value=None)

    def read_number_opt(self, key: str, default_value: Number | None = None) -> Number | None:
        """
        An appropriate error log is generated if the value is not a number.
        :return: a Number if the value for the key is a YAML number, otherwise None.
        """
        return self.read_value(key=key, expected_type=Number, required=False, default_value=default_value)

    def read_value(
        self,
        key: str,
        expected_type: type = None,
        required: bool = False,
        default_value=None,
    ) -> object | None:
        if key not in self.yaml_dict:
            if required:
                self.yaml_file.logs.error(
                    message=f"'{key}' is required",
                    location=self.create_location_from_yaml_dict_key(key)
                )
            return default_value
        value = self.yaml_dict.get(key)
        if expected_type is not None and not isinstance(value, expected_type):
            self.yaml_file.logs.error(
                message=f"'{key}' expected a {expected_type.__name__}, but was {type(value).__name__}",
                location=self.create_location_from_yaml_dict_key(key)
            )
        return self._yaml_wrap(value)

    def create_location_from_yaml_dict_key(self, key) -> Location | None:
        if isinstance(self.yaml_dict, CommentedMap):
            if key in self.yaml_dict:
                ruamel_location = self.yaml_dict.lc.value(key)
                line: int = ruamel_location[0]
                column: int = ruamel_location[1]
                return Location(file_path=self.yaml_file.description, line=line, column=column)
            else:
                return self.create_location_from_yaml_value()
        return None

    def create_location_from_yaml_value(self) -> Location | None:
        if isinstance(self.yaml_dict, CommentedMap) or isinstance(self.yaml_dict, CommentedSeq):
            return Location(
                file_path=self.yaml_file.description,
                line=self.yaml_dict.lc.line,
                column=self.yaml_dict.lc.col
            )
        return None

    def to_dict(self) -> dict:
        return YamlValue.yaml_unwrap(self)


class YamlList(YamlValue, Iterable):

    def __init__(self, yaml_file: YamlFile, yaml_list: list) -> None:
        super().__init__(yaml_file)
        self.yaml_list: list = [self._yaml_wrap(element) for element in yaml_list]

    def __iter__(self) -> iter:
        return iter(self.yaml_list)

    def to_list(self) -> list:
        return YamlValue.yaml_unwrap(self)


class VariableResolver:

    def __init__(self, variables: dict[str, str] | None = None, logs: Logs | None = None):
        self.variables: dict[str, str] | None = variables
        self.logs: Logs = logs if logs else Logs()

    def resolve(self, text: str) -> str:
        return re.sub(
            pattern=r"\$\{([a-zA-Z_][a-zA-Z_0-9]*)\}",
            repl=lambda m: self._resolve_variable(m.group(1).strip()),
            string=text,
        )

    def _resolve_variable(self, variable_name: str) -> str:
        if self.variables is not None and variable_name in self.variables:
            return self.variables[variable_name]
        if variable_name in os.environ:
            return os.getenv(variable_name)
        self.logs.error(f"Variable '{variable_name}' not defined in the variables nor as environment variable")
        return ""
