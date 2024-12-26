from __future__ import annotations

import os
import re
from abc import abstractmethod
from numbers import Number
from typing import Iterable

from ruamel.yaml import YAML, CommentedMap, CommentedSeq
from ruamel.yaml.error import MarkedYAMLError

from soda_core.common.logs import Logs, Location


class YamlParser:

    def __init__(self):
        self.ruamel_yaml_parser: YAML = YAML()
        self.ruamel_yaml_parser.preserve_quotes = True


class YamlSource:

    @classmethod
    def from_str(cls, yaml_str: str) -> YamlSource:
        return StrYamlSource(
            yaml_str=yaml_str
        )

    @classmethod
    def from_file_path(cls, yaml_file_path: str) -> YamlSource:
        return FileYamlSource(
            yaml_file_path=yaml_file_path
        )

    @classmethod
    def from_dict(cls, yaml_dict: dict) -> YamlSource:
        return DictYamlSource(
            yaml_dict=yaml_dict
        )

    @abstractmethod
    def parse_yaml_file_content(
        self, file_type: str, variables: dict | None = None, logs: Logs = None
    ) -> YamlFileContent:
        pass


class FileYamlSource(YamlSource):

    def __init__(self, yaml_file_path: str):
        super().__init__()
        self.yaml_file_path: str = yaml_file_path

    def parse_yaml_file_content(self, file_type: str, variables: dict | None = None, logs: Logs | None = None) -> YamlFileContent:
        logs = logs if logs else Logs()
        yaml_source_description: str = f"{file_type} {self.yaml_file_path}"
        yaml_str: str | None = YamlFileContent.read_yaml_file(
            file_path=self.yaml_file_path, yaml_source_description=yaml_source_description, logs=logs
        )
        yaml_str_resolved: str = YamlFileContent.resolve_yaml_str(yaml_str=yaml_str, variables=variables)
        yaml_dict: dict | None = YamlFileContent.parse_yaml_dict(
            yaml_str=yaml_str_resolved, yaml_file_path=self.yaml_file_path,
            yaml_source_description=yaml_source_description, logs=logs
        )
        return YamlFileContent(
            yaml_file_path=self.yaml_file_path,
            yaml_source_description=yaml_source_description,
            yaml_str_source=yaml_str,
            yaml_str_resolved=yaml_str_resolved,
            yaml_dict=yaml_dict,
            logs=logs,
        )


class StrYamlSource(YamlSource):

    def __init__(self, yaml_str: str):
        super().__init__()
        self.yaml_str: str = yaml_str

    def parse_yaml_file_content(self, file_type: str, variables: dict | None = None, logs: Logs | None = None) -> YamlFileContent:
        logs = logs if logs else Logs()
        yaml_source_description: str = f"{file_type} yaml string"
        yaml_str_resolved: str = YamlFileContent.resolve_yaml_str(yaml_str=self.yaml_str, variables=variables)
        yaml_dict: dict | None = YamlFileContent.parse_yaml_dict(
            yaml_str=yaml_str_resolved, yaml_file_path=yaml_source_description,
            yaml_source_description=yaml_source_description, logs=logs
        )
        return YamlFileContent(
            yaml_file_path=None,
            yaml_source_description=yaml_source_description,
            yaml_str_source=self.yaml_str,
            yaml_str_resolved=yaml_str_resolved,
            yaml_dict=yaml_dict,
            logs=logs,
        )


class DictYamlSource(YamlSource):

    def __init__(self, yaml_dict: dict):
        super().__init__()
        self.yaml_dict: dict = yaml_dict

    def parse_yaml_file_content(self, file_type: str, variables: dict | None = None, logs: Logs | None = None) -> YamlFileContent:
        logs = logs if logs else Logs()
        return YamlFileContent(
            yaml_file_path=None,
            yaml_source_description=f"{file_type} yaml dict",
            yaml_str_source=None,
            yaml_str_resolved=None,
            yaml_dict=self.yaml_dict,
            logs=logs,
        )


class YamlFileContent:

    __yaml_parser = YamlParser()

    def __init__(
        self,
        yaml_file_path: str | None,
        yaml_source_description: str | None,
        yaml_str_source: str | None,
        yaml_str_resolved: str | None,
        yaml_dict: dict | None,
        logs: Logs,
    ):
        self.yaml_file_path: str | None = yaml_file_path
        self.yaml_source_description: str | None = yaml_source_description
        self.yaml_str_source: str | None = yaml_str_source
        self.yaml_str_resolved: str | None = yaml_str_resolved
        self.yaml_dict: dict | None = yaml_dict
        self.logs: Logs = logs

    @classmethod
    def read_yaml_file(cls, file_path: str, yaml_source_description: str, logs: Logs) -> str | None:
        if isinstance(file_path, str):
            try:
                with open(file_path) as f:
                    return f.read()
            except OSError as e:
                if not os.path.exists(file_path):
                    logs.error(message=f"{yaml_source_description} does not exist")
                elif not os.path.isdir(file_path):
                    logs.error(message=f"{yaml_source_description} is a directory")
                else:
                    logs.error(message=f"{yaml_source_description} can't be read", exception=e)

    @classmethod
    def resolve_yaml_str(cls, yaml_str: str, variables: dict | None) -> str | None:
        return VariableResolver.resolve(source_text_with_variables=yaml_str, variables=variables)

    @classmethod
    def parse_yaml_dict(cls, yaml_str: str, yaml_file_path: str | None, yaml_source_description: str, logs: Logs) -> any:
        if isinstance(yaml_str, str):
            try:
                root_yaml_object: any = cls.__yaml_parser.ruamel_yaml_parser.load(yaml_str)
                if isinstance(root_yaml_object, dict):
                    return root_yaml_object
                else:
                    location = Location(file_path=yaml_file_path, line=0, column=0)
                    logs.error(
                        message=f"Root YAML in {yaml_source_description} is not an object, "
                                f"but was: {root_yaml_object.__class__.__name__}",
                        location=location
                    )
                    return None

            except MarkedYAMLError as e:
                mark = e.context_mark if e.context_mark else e.problem_mark
                line = mark.line + 1
                col = mark.column + 1
                location = Location(file_path=yaml_file_path, line=line, column=col)
                logs.error(message=f"YAML syntax error in {yaml_source_description}", exception=e, location=location)

    def get_yaml_object(self) -> YamlObject | None:
        return YamlObject(self, self.yaml_dict) if isinstance(self.yaml_dict, dict) else None

    def has_yaml_object(self) -> bool:
        return isinstance(self.yaml_dict, dict)

    def has_errors(self) -> bool:
        return self.logs.has_errors()

    def assert_no_errors(self) -> None:
        if self.logs.has_errors():
            raise AssertionError(f"{self.yaml_source_description} has errors: {self.logs}")

    def __str__(self) -> str:
        return self.yaml_source_description


class YamlValue:

    def __init__(self, yaml_file_content: YamlFileContent) -> None:
        self.yaml_file_content: YamlFileContent = yaml_file_content
        self.logs: Logs = yaml_file_content.logs

    def _yaml_wrap(self, value):
        if isinstance(value, dict):
            return YamlObject(yaml_file_content=self.yaml_file_content, yaml_dict=value)
        if isinstance(value, list):
            return YamlList(yaml_file_content=self.yaml_file_content, yaml_list=value)
        return value

    @classmethod
    def yaml_unwrap(cls, o) -> object | None:
        if isinstance(o, YamlObject):
            return {k: cls.yaml_unwrap(v) for k, v in o.items()}
        if isinstance(o, YamlList):
            return [cls.yaml_unwrap(e) for e in o]
        return o


class YamlObject(YamlValue):

    def __init__(self, yaml_file_content: YamlFileContent, yaml_dict: dict) -> None:
        super().__init__(yaml_file_content)
        self.yaml_dict: dict = yaml_dict

    def items(self) -> list[tuple]:
        return [(k,self._yaml_wrap(v)) for k, v in self.yaml_dict.items()]

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
                self.logs.error(
                    message=f"'{key}' is required",
                    location=self.create_location_from_yaml_dict_key(key)
                )
            return default_value
        value = self.yaml_dict.get(key)
        if expected_type is not None and not isinstance(value, expected_type):
            self.logs.error(
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
                return Location(file_path=self.yaml_file_content.yaml_file_path, line=line, column=column)
            else:
                return self.create_location_from_yaml_value()
        return None

    def create_location_from_yaml_value(self) -> Location | None:
        if isinstance(self.yaml_dict, CommentedMap) or isinstance(self.yaml_dict, CommentedSeq):
            return Location(
                file_path=self.yaml_file_content.yaml_file_path,
                line=self.yaml_dict.lc.line,
                column=self.yaml_dict.lc.col
            )
        return None

    def to_dict(self) -> dict:
        return YamlValue.yaml_unwrap(self)


class YamlList(YamlValue, Iterable):

    def __init__(self, yaml_file_content: YamlFileContent, yaml_list: list) -> None:
        super().__init__(yaml_file_content)
        self.yaml_list: list = [self._yaml_wrap(element) for element in yaml_list]

    def __iter__(self) -> iter:
        return iter(self.yaml_list)

    def to_list(self) -> list:
        return YamlValue.yaml_unwrap(self)


class VariableResolver:

    @classmethod
    def resolve(cls, source_text_with_variables: str, variables: dict[str, str] | None) -> str:
        if isinstance(source_text_with_variables, str):
            return re.sub(
                pattern=r"\$\{([a-zA-Z_][a-zA-Z_0-9]*)\}",
                repl=lambda m: cls._resolve_variable(variables=variables, variable=m.group(1).strip()),
                string=source_text_with_variables,
            )
        else:
            return source_text_with_variables

    @classmethod
    def _resolve_variable(cls, variables: dict[str, str], variable: str) -> str:
        if isinstance(variables, dict) and variable in variables:
            return variables[variable]
        if variable in os.environ:
            return os.getenv(variable)
        return f"{{{variable}}}"
