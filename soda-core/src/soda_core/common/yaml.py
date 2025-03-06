from __future__ import annotations

import os
import re
from abc import abstractmethod
from numbers import Number
from typing import Iterable, Optional

from ruamel.yaml import YAML, CommentedMap, CommentedSeq
from ruamel.yaml.error import MarkedYAMLError

from soda_core.common.logs import Logs, Location, Emoticons


class YamlParser:

    def __init__(self):
        self.ruamel_yaml_parser: YAML = YAML()
        self.ruamel_yaml_parser.preserve_quotes = True


class YamlSource:

    @classmethod
    def from_str(cls, yaml_str: str) -> YamlSource:
        return StrYamlSource(yaml_str=yaml_str)

    @classmethod
    def from_file_path(cls, yaml_file_path: str) -> YamlSource:
        return FileYamlSource(yaml_file_path=yaml_file_path)

    @classmethod
    def from_dict(cls, yaml_dict: dict) -> YamlSource:
        return DictYamlSource(yaml_dict=yaml_dict)

    @abstractmethod
    def parse_yaml_file_content(
        self, file_type: Optional[str] = None, variables: Optional[dict] = None, logs: Logs = None
    ) -> YamlFileContent:
        pass


class FileYamlSource(YamlSource):

    def __init__(self, yaml_file_path: str):
        super().__init__()
        self.yaml_file_path: str = yaml_file_path

    def __str__(self) -> str:
        return self.yaml_file_path

    def parse_yaml_file_content(self, file_type: Optional[str] = None, variables: Optional[dict] = None, logs: Optional[Logs] = None) -> YamlFileContent:
        logs = logs if logs else Logs()
        yaml_source_description: str = (f"{file_type} file '{self.yaml_file_path}'" if file_type
                                        else f"'{self.yaml_file_path}'")
        yaml_str: Optional[str] = YamlFileContent.read_yaml_file(
            file_path=self.yaml_file_path, yaml_source_description=yaml_source_description, logs=logs
        )
        yaml_str_resolved: str = YamlFileContent.resolve_yaml_str(yaml_str=yaml_str, variables=variables)
        yaml_dict: Optional[dict] = YamlFileContent.parse_yaml_dict(
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

    def __init__(self, yaml_str: str, file_path: Optional[str] = "yaml_string.yml"):
        super().__init__()
        self.yaml_str: str = yaml_str
        self.file_path: Optional[str] = file_path

    def __str__(self) -> str:
        return self.file_path if isinstance(self.file_path, str) else "yaml_str"

    def parse_yaml_file_content(self, file_type: Optional[str] = None, variables: Optional[dict] = None, logs: Optional[Logs] = None) -> YamlFileContent:
        logs = logs if logs else Logs()
        yaml_source_description: str = f"{file_type} yaml string" if file_type else "yaml string"
        yaml_str_resolved: str = YamlFileContent.resolve_yaml_str(yaml_str=self.yaml_str, variables=variables)
        yaml_dict: Optional[dict] = YamlFileContent.parse_yaml_dict(
            yaml_str=yaml_str_resolved, yaml_file_path=yaml_source_description,
            yaml_source_description=yaml_source_description, logs=logs
        )
        return YamlFileContent(
            yaml_file_path=self.file_path,
            yaml_source_description=yaml_source_description,
            yaml_str_source=self.yaml_str,
            yaml_str_resolved=yaml_str_resolved,
            yaml_dict=yaml_dict,
            logs=logs,
        )


class DictYamlSource(YamlSource):

    def __init__(self, yaml_dict: dict, file_path: Optional[str] = "yaml_dict.yml"):
        super().__init__()
        self.yaml_dict: dict = yaml_dict
        self.file_path: Optional[str] = file_path

    def __str__(self) -> str:
        return self.file_path if isinstance(self.file_path, str) else "yaml_dict"

    def parse_yaml_file_content(self, file_type: Optional[str] = None, variables: Optional[dict] = None, logs: Optional[Logs] = None) -> YamlFileContent:
        logs = logs if logs else Logs()
        yaml_source_description: str = f"{file_type} yaml dict" if file_type else "yaml dict"
        return YamlFileContent(
            yaml_file_path=self.file_path,
            yaml_source_description=yaml_source_description,
            yaml_str_source=None,
            yaml_str_resolved=None,
            yaml_dict=self.yaml_dict,
            logs=logs,
        )


class YamlFileContent:

    __yaml_parser = YamlParser()

    def __init__(
        self,
        yaml_file_path: Optional[str],
        yaml_source_description: Optional[str],
        yaml_str_source: Optional[str],
        yaml_str_resolved: Optional[str],
        yaml_dict: Optional[dict],
        logs: Logs,
    ):
        self.yaml_file_path: Optional[str] = yaml_file_path
        self.yaml_source_description: Optional[str] = yaml_source_description
        self.yaml_str_source: Optional[str] = yaml_str_source
        self.yaml_str_resolved: Optional[str] = yaml_str_resolved
        self.yaml_dict: Optional[dict] = yaml_dict
        self.logs: Logs = logs

    @classmethod
    def read_yaml_file(cls, file_path: str, yaml_source_description: str, logs: Logs) -> Optional[str]:
        if isinstance(file_path, str):
            try:
                with open(file_path) as f:
                    file_content_str: str = f.read()
                    if len(file_content_str) > 0:
                        return file_content_str
                    else:
                        logs.error(message=f"{Emoticons.POLICE_CAR_LIGHT} {yaml_source_description} is empty")

            except OSError as e:
                if not os.path.exists(file_path):
                    logs.error(message=f"{Emoticons.POLICE_CAR_LIGHT} {yaml_source_description} does not exist")
                elif not os.path.isdir(file_path):
                    logs.error(message=f"{Emoticons.POLICE_CAR_LIGHT} {yaml_source_description} is a directory")
                else:
                    logs.error(
                        message=f"{Emoticons.POLICE_CAR_LIGHT} {yaml_source_description} can't be read",
                        exception=e
                    )

    @classmethod
    def resolve_yaml_str(cls, yaml_str: str, variables: Optional[dict]) -> Optional[str]:
        return VariableResolver.resolve(source_text_with_variables=yaml_str, variables=variables)

    @classmethod
    def parse_yaml_dict(cls, yaml_str: str, yaml_file_path: Optional[str], yaml_source_description: str, logs: Logs) -> any:
        if isinstance(yaml_str, str):
            try:
                root_yaml_object: any = cls.__yaml_parser.ruamel_yaml_parser.load(yaml_str)
                if isinstance(root_yaml_object, dict):
                    return root_yaml_object
                else:
                    location = Location(file_path=yaml_file_path, line=0, column=0)
                    yaml_type: str = root_yaml_object.__class__.__name__ if root_yaml_object is not None else "empty"
                    yaml_type = "a list" if yaml_type == "CommentedSeq" else yaml_type
                    logs.error(
                        message=f"Root YAML in {yaml_source_description} must be an object, "
                                f"but was {yaml_type}",
                        location=location
                    )
                    return None

            except MarkedYAMLError as e:
                mark = e.context_mark if e.context_mark else e.problem_mark
                line = mark.line + 1
                col = mark.column + 1
                location = Location(file_path=yaml_file_path, line=line, column=col)
                logs.error(message=f"YAML syntax error in {yaml_source_description}", exception=e, location=location)

    def get_yaml_object(self) -> Optional[YamlObject]:
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
    def yaml_unwrap(cls, o) -> Optional[object]:
        if isinstance(o, YamlObject):
            return {k: cls.yaml_unwrap(v) for k, v in o.items()}
        if isinstance(o, YamlList):
            return [cls.yaml_unwrap(e) for e in o]
        return o


def get_location(yaml_value: any, yaml_file_path: Optional[str]) -> Optional[Location]:
    if isinstance(yaml_value, CommentedMap) or isinstance(yaml_value, CommentedSeq):
        return Location(
            file_path=yaml_file_path,
            line=yaml_value.lc.line,
            column=yaml_value.lc.col
        )


class YamlObject(YamlValue):

    def __init__(self, yaml_file_content: YamlFileContent, yaml_dict: dict) -> None:
        super().__init__(yaml_file_content)
        self.yaml_dict: dict = yaml_dict
        self.location: Optional[Location] = get_location(self.yaml_dict, yaml_file_content.yaml_file_path)

    def items(self) -> list[tuple]:
        return [(k,self._yaml_wrap(v)) for k, v in self.yaml_dict.items()]

    def keys(self) -> set[str]:
        return set(self.yaml_dict.keys())

    def __iter__(self) -> iter:
        return iter(self.yaml_dict.keys())

    def has_key(self, key: str) -> bool:
        return key in self.yaml_dict

    def read_object(self, key: str) -> Optional[YamlObject]:
        """
        An error is generated if the value is missing or not a YAML object.
        :return: a dict if the value for the key is a YAML object, otherwise None.
        """
        return self.read_value(key=key, expected_type=dict, required=True, default_value=None)

    def read_object_opt(self, key: str, default_value: Optional[dict] = None) -> Optional[YamlObject]:
        """
        An error is generated if the value is present and not a YAML object.
        :return: a dict if the value for the key is a YAML object, otherwise None.
        """
        return self.read_value(key=key, expected_type=dict, required=False, default_value=default_value)

    def read_list(self, key: str, expected_element_type: Optional[type] = None, required: bool = True) -> Optional[YamlList]:
        """
        An error is generated if the value is missing or not a YAML list.
        :return: a list if the value for the key is a YAML list, otherwise None.
        """
        list_value: Optional[YamlList] = self.read_value(key=key, expected_type=list, required=required, default_value=None)
        if isinstance(list_value, YamlList):
            filtered_list: list = []
            if isinstance(expected_element_type, type):
                for index, element in enumerate(list_value.yaml_list):
                    if isinstance(element, expected_element_type):
                        filtered_list.append(element)
                    else:
                        self.logs.error(
                            message=f"{Emoticons.POLICE_CAR_LIGHT} YAML key '{key}' expects a "
                                    f"list of {expected_element_type.__name__}. "
                                    f"But element {index} was {type(element).__name__}",
                            location=self.create_location_from_yaml_dict_key(key)
                        )
            list_value.yaml_list = filtered_list
        return list_value

    def read_list_opt(self, key: str) -> Optional[YamlList]:
        """
        An error is generated if the value is present and not a YAML list.
        :return: a list if the value for the key is a YAML list, otherwise None.
        """
        return self.read_value(key=key, expected_type=list, required=False, default_value=None)

    def read_list_of_objects_opt(self, key: str) -> Optional[YamlList]:
        return self.read_list_of_objects(key) if key in self.yaml_dict else None

    def read_list_of_objects(self, key: str) -> Optional[YamlList]:
        list_value: Optional[YamlList] = self.read_list(key=key, expected_element_type=YamlObject)
        if isinstance(list_value, YamlList):
            for i in range(0, len(list_value.yaml_list)):
                element = list_value.yaml_list[i]
                if not isinstance(element, YamlObject):
                    self.logs.error(
                        message=f"{Emoticons.POLICE_CAR_LIGHT} YAML key '{key}' expected list of objects.  "
                                f"But element {i} was {type(element).__name__}",
                        location=self.create_location_from_yaml_dict_key(key)
                    )
                    list_value.yaml_list[i] = None
        return list_value

    def read_list_of_strings(self, key: str) -> list[str] | None:
        l = self.read_list(key, expected_element_type=str)
        return l.to_list() if isinstance(l, YamlList) else None

    def read_list_of_strings_opt(self, key: str) -> list[str] | None:
        l = self.read_list(key, expected_element_type=str, required=False)
        return l.to_list() if isinstance(l, YamlList) else None

    def read_string(self, key: str, env_var: Optional[str] = None) -> Optional[str]:
        """
        An error is generated if the value is missing or not a string.
        :return: a str if the value for the key is a YAML string, otherwise None.
        """
        return self.read_value(key=key, env_var=env_var, expected_type=str, required=True, default_value=None)

    def read_string_opt(self, key: str, env_var: Optional[str] = None, default_value: Optional[str] = None) -> Optional[str]:
        """
        An error is generated if the value is present and not a string.
        :return: a str if the value for the key is a string, otherwise None.
        """
        return self.read_value(
            key=key, env_var=env_var, expected_type=str, required=False, default_value=default_value
        )

    def read_bool(self, key: str) -> Optional[bool]:
        """
        An appropriate error log is generated if the value is not a bool or if the key is missing
        :return: a bool if the value for the key is a YAML boolean, otherwise None.
        """
        return self.read_value(key=key, expected_type=bool, required=True, default_value=None)

    def read_bool_opt(self, key: str, default_value: Optional[bool] = None) -> Optional[bool]:
        """
        An appropriate error log is generated if the value is not a bool.
        :return: a bool if the value for the key is a YAML boolean, otherwise None.
        """
        return self.read_value(key=key, expected_type=bool, required=False, default_value=default_value)

    def read_number(self, key: str) -> Optional[Number]:
        """
        An appropriate error log is generated if the value is not a number or if the key is missing
        :return: a bool if the value for the key is a YAML number, otherwise None.
        """
        return self.read_value(key=key, expected_type=Number, required=True, default_value=None)

    def read_number_opt(self, key: str, default_value: Optional[Number] = None) -> Optional[Number]:
        """
        An appropriate error log is generated if the value is not a number.
        :return: a Number if the value for the key is a YAML number, otherwise None.
        """
        return self.read_value(key=key, expected_type=Number, required=False, default_value=default_value)

    def read_value(
        self,
        key: str,
        env_var: Optional[str] = None,
        expected_type: type = None,
        required: bool = False,
        default_value=None,
    ) -> Optional[object]:
        key_description: str = f"YAML key '{key}'"
        if env_var is not None and env_var in os.environ:
            key_description = f"Env var '{env_var}'"
            value = os.environ[env_var]
        elif key in self.yaml_dict:
            value = self.yaml_dict.get(key)
        else:
            if required:
                self.logs.error(
                    message=f"{Emoticons.POLICE_CAR_LIGHT} {key_description} is required",
                    location=self.location
                )
            value = default_value

        if expected_type is not None and not isinstance(value, expected_type) and value != default_value:
            actual_type_str: str = type(value).__name__
            if value is None:
                actual_type_str = "None"
            elif isinstance(value, dict):
                actual_type_str = "YAML object"
            elif isinstance(value, list):
                actual_type_str = "YAML list"
            self.logs.error(
                message=f"{Emoticons.POLICE_CAR_LIGHT} {key_description} expected a {expected_type.__name__}, "
                        f"but was {actual_type_str}",
                location=self.create_location_from_yaml_dict_key(key)
            )
            value = None

        return self._yaml_wrap(value)

    def create_location_from_yaml_dict_key(self, key) -> Optional[Location]:
        if isinstance(self.yaml_dict, CommentedMap):
            if key in self.yaml_dict:
                ruamel_location = self.yaml_dict.lc.value(key)
                line: int = ruamel_location[0]
                column: int = ruamel_location[1]
                return Location(file_path=self.yaml_file_content.yaml_file_path, line=line, column=column)

    def to_dict(self) -> dict:
        return YamlValue.yaml_unwrap(self)


class YamlList(YamlValue, Iterable):

    def __init__(self, yaml_file_content: YamlFileContent, yaml_list: list) -> None:
        super().__init__(yaml_file_content)
        self.yaml_list: list = [self._yaml_wrap(element) for element in yaml_list]
        self.location: Optional[Location] = get_location(yaml_list, yaml_file_content.yaml_file_path)

    def __iter__(self) -> iter:
        return iter(self.yaml_list)

    def to_list(self) -> list:
        return YamlValue.yaml_unwrap(self)


class VariableResolver:

    @classmethod
    def resolve(cls, source_text_with_variables: str, variables: Optional[dict[str, str]] = None) -> str:
        if isinstance(source_text_with_variables, str):
            return re.sub(
                pattern=r"\$\{ *([a-zA-Z_][a-zA-Z_0-9]*)\ *}",
                repl=lambda m: cls._resolve_variable_pattern(variables=variables, variable=m.group(1).strip()),
                string=source_text_with_variables,
            )
        else:
            return source_text_with_variables

    @classmethod
    def _resolve_variable_pattern(cls, variables: dict[str, str], variable: str) -> str:
        return cls.get_variable(variables=variables, variable=variable, default=f"${{{variable}}}")

    @classmethod
    def get_variable(cls, variables: dict[str, str], variable: str, default: Optional[str] = None) -> Optional[str]:
        if isinstance(variables, dict) and variable in variables:
            return variables[variable]
        if variable in os.environ:
            return os.getenv(variable)
        return default
