from __future__ import annotations

import logging
import os
import re
from enum import Enum
from io import StringIO
from numbers import Number
from typing import Iterable, Optional

from ruamel.yaml import YAML, CommentedMap, CommentedSeq
from ruamel.yaml.error import MarkedYAMLError
from soda_core.common.logging_constants import ExtraKeys, soda_logger
from soda_core.common.logs import Location

logger: logging.Logger = soda_logger


class YamlParser:
    def __init__(self):
        self.ruamel_yaml_parser: YAML = YAML()
        self.ruamel_yaml_parser.preserve_quotes = True

    def dump_to_string(self, data):
        stream = StringIO()
        self.ruamel_yaml_parser.dump(data, stream)
        return stream.getvalue()


class FileType(str, Enum):
    DATA_SOURCE = "Data Source"
    SODA_CLOUD = "Soda Cloud"
    CONTRACT = "Contract"
    YAML = "YAML"


class YamlSource:
    """
    YamlSource is an abstraction for multiple types of YAML sources: see subclasses below.
    It does lazy loading of the file so that errors are only generated when resolving or parsing.
    It's a stateful object that allows for optional variable resolving.

    Usage:


    yaml_source: YamlSource = ContractYamlSource.from_str(
        yaml_str="...yaml...",
        file_path="./some/contract.yml" # optional
    )
    # or
    yaml_source: YamlSource = ContractYamlSource.from_file_path(
        file_path="./some/contract.yml"
    )

    # The default file type is 'YAML'.
    # Specific subclasses use different file types.

    # Optionally resolve variables as many times as you like:
    # resolving will trigger file loading (if applicable)
    yaml_source.resolve(variables={}, use_env_vars=True)

    # parse will create the YamlObject
    # resolving will trigger file loading (if applicable)
    yaml_object: YamlObject = yaml_source.parse()
    """

    __yaml_parser = YamlParser()
    __file_type = FileType.YAML.value

    @classmethod
    def from_str(cls, yaml_str: str, file_path: Optional[str] = None) -> YamlSource:
        """
        Raises an assertion exception if yaml_str is not a str
        """
        assert isinstance(yaml_str, str)
        return cls(file_path=file_path, yaml_str=yaml_str)

    @classmethod
    def from_file_path(cls, file_path: str) -> YamlSource:
        """
        Raises an assertion exception if file_path is not a str
        """
        assert isinstance(file_path, str)
        return cls(file_path=file_path)

    def __init__(self, file_path: Optional[str] = None, yaml_str: Optional[str] = None):
        self.file_type: str = self.__file_type
        self.file_path: str = file_path
        self.description: str = self._build_description(self.file_type, self.file_path)
        self.is_file_read: bool = False
        self.yaml_str: Optional[str] = yaml_str
        self.yaml_str_original: Optional[str] = yaml_str
        self.resolve_on_read: bool = False
        self.resolve_on_read_variables: Optional[dict[str, str]] = None
        self.resolve_on_read_use_env_vars: bool = True
        self.resolve_on_read_ignored_variable_names: Optional[set[str]] = None

    def __init_subclass__(cls, file_type: FileType, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.__file_type = file_type.value

    def __str__(self) -> str:
        return self.description

    @classmethod
    def _build_description(cls, file_type: str, file_path: Optional[str]) -> str:
        if file_type:
            if file_path:
                return f"{file_type} file '{file_path}'"
            else:
                return f"{file_type} YAML string"
        else:
            if file_path:
                return f"YAML file '{file_path}'"
            else:
                return f"YAML string"

    def _ensure_yaml_str(self) -> None:
        if not isinstance(self.yaml_str, str) and isinstance(self.file_path, str) and not self.is_file_read:
            # There should be only 1 reading attempt.
            self.is_file_read = True
            try:
                with open(self.file_path) as f:
                    self.yaml_str = f.read()
                    self.yaml_str_original = self.yaml_str
                    if len(self.yaml_str) == 0:
                        logger.error(f"{self.description} is empty")

            except OSError as e:
                if not os.path.exists(self.file_path):
                    logger.error(f"{self.description} does not exist")
                elif not os.path.isdir(self.file_path):
                    logger.error(f"{self.description} is a directory")
                else:
                    logger.error(f"{self.description} can't be read", exc_info=True)

    def resolve(self, variables: Optional[dict[str, str]] = None, use_env_vars: bool = True) -> None:
        """
        Note: this does not change the object, but returns a new ResolvedYamlSource instead
        """
        self._ensure_yaml_str()
        self.yaml_str = VariableResolver.resolve(
            source_text=self.yaml_str, variables=variables, use_env_vars=use_env_vars
        )

    def resolve_on_read_value(self, variables: dict[str, str], ignored_variable_names: set[str], use_env_vars: bool):
        self.resolve_on_read = True
        self.resolve_on_read_variables = variables
        self.resolve_on_read_use_env_vars = use_env_vars
        self.resolve_on_read_ignored_variable_names = ignored_variable_names

    def parse(self) -> Optional[YamlObject]:
        self._ensure_yaml_str()
        if isinstance(self.yaml_str, str):
            try:
                root_yaml_object: any = self.__yaml_parser.ruamel_yaml_parser.load(self.yaml_str)
                if isinstance(root_yaml_object, dict):
                    return YamlObject(self, root_yaml_object)
                else:
                    location = Location(file_path=self.file_path, line=0, column=0)
                    yaml_type: str = root_yaml_object.__class__.__name__ if root_yaml_object is not None else "empty"
                    yaml_type = "a list" if yaml_type == "CommentedSeq" else yaml_type
                    logger.error(
                        msg=f"{self.description} root must be an object, " f"but was {yaml_type}",
                        extra={
                            ExtraKeys.LOCATION: location,
                        },
                    )

            except MarkedYAMLError as e:
                mark = e.context_mark if e.context_mark else e.problem_mark
                line = mark.line + 1
                col = mark.column + 1
                location = Location(file_path=self.file_path, line=line, column=col)
                logger.error(
                    msg=f"YAML syntax error in {self.description}",
                    exc_info=True,
                    extra={ExtraKeys.LOCATION: location},
                )


class DataSourceYamlSource(YamlSource, file_type=FileType.DATA_SOURCE):
    ...


class SodaCloudYamlSource(YamlSource, file_type=FileType.SODA_CLOUD):
    ...


class ContractYamlSource(YamlSource, file_type=FileType.CONTRACT):
    ...


class YamlValue:
    def __init__(self, yaml_source: YamlSource) -> None:
        self.yaml_source: YamlSource = yaml_source

    def _yaml_wrap(self, value: any, location: Optional[Location] = None):
        if isinstance(value, dict):
            return YamlObject(yaml_source=self.yaml_source, yaml_dict=value)
        if isinstance(value, list):
            return YamlList(yaml_source=self.yaml_source, yaml_list=value)
        if isinstance(value, str) and self.yaml_source.resolve_on_read:
            value = VariableResolver.resolve(
                source_text=value,
                variables=self.yaml_source.resolve_on_read_variables,
                use_env_vars=self.yaml_source.resolve_on_read_use_env_vars,
                ignored_variable_names=self.yaml_source.resolve_on_read_ignored_variable_names,
                location=location,
            )
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
        return Location(file_path=yaml_file_path, line=yaml_value.lc.line, column=yaml_value.lc.col)


class YamlObject(YamlValue):
    def __init__(self, yaml_source: YamlSource, yaml_dict: dict) -> None:
        super().__init__(yaml_source)
        self.yaml_dict: dict = yaml_dict
        self.location: Optional[Location] = get_location(self.yaml_dict, yaml_source.file_path)

    def items(self) -> list[tuple]:
        return [(k, self._yaml_wrap(v)) for k, v in self.yaml_dict.items()]

    def keys(self) -> list[str]:
        return list(self.yaml_dict.keys())

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

    def read_list(
        self, key: str, expected_element_type: Optional[type] = None, required: bool = True
    ) -> Optional[YamlList]:
        """
        An error is generated if the value is missing or not a YAML list.
        :return: a list if the value for the key is a YAML list, otherwise None.
        """
        list_value: Optional[YamlList] = self.read_value(
            key=key, expected_type=list, required=required, default_value=None
        )
        if isinstance(list_value, YamlList):
            filtered_list: list = []
            if isinstance(expected_element_type, type):
                for index, element in enumerate(iter(list_value)):
                    if isinstance(element, expected_element_type):
                        filtered_list.append(element)
                    else:
                        logger.error(
                            msg=(
                                f"YAML key '{key}' expects a "
                                f"list of {expected_element_type.__name__}. "
                                f"But element {index} was {type(element).__name__}"
                            ),
                            extra={
                                ExtraKeys.LOCATION: self.create_location_from_yaml_dict_key(key),
                            },
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
                    logger.error(
                        msg=(
                            f"YAML key '{key}' expected list of objects.  "
                            f"But element {i} was {type(element).__name__}"
                        ),
                        extra={
                            ExtraKeys.LOCATION: self.create_location_from_yaml_dict_key(key),
                        },
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

    def read_string_opt(
        self, key: str, env_var: Optional[str] = None, default_value: Optional[str] = None
    ) -> Optional[str]:
        """
        An error is generated if the value is present and not a string.
        :return: a str if the value for the key is a string, otherwise None.
        """
        return self.read_value(key=key, env_var=env_var, expected_type=str, required=False, default_value=default_value)

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
        location: Location = self.create_location_from_yaml_dict_key(key) if key in self.yaml_dict else self.location

        key_description: str = f"YAML key '{key}'"
        if env_var is not None and env_var in os.environ:
            key_description = f"Env var '{env_var}'"
            if key in self.yaml_dict:
                logger.debug(f"Using environment var '{env_var}' instead of configuration key '{key}'")
            value = os.environ[env_var]
        elif key in self.yaml_dict:
            value = self.yaml_dict.get(key)
        else:
            if required:
                logger.error(
                    msg=f"{key_description} is required",
                    extra={
                        ExtraKeys.LOCATION: self.location,
                    },
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
            logger.error(
                msg=(f"{key_description} expected a {expected_type.__name__}, " f"but was {actual_type_str}"),
                extra={
                    ExtraKeys.LOCATION: location,
                },
            )
            value = None

        return self._yaml_wrap(value, location=location)

    def create_location_from_yaml_dict_key(self, key) -> Optional[Location]:
        if isinstance(self.yaml_dict, CommentedMap):
            if key in self.yaml_dict:
                ruamel_location = self.yaml_dict.lc.value(key)
                line: int = ruamel_location[0]
                column: int = ruamel_location[1]
                return Location(file_path=self.yaml_source.file_path, line=line, column=column)

    def to_dict(self) -> dict:
        return self.yaml_dict


class YamlList(YamlValue, Iterable):
    def __init__(self, yaml_source: YamlSource, yaml_list: list) -> None:
        super().__init__(yaml_source)
        self.yaml_list: list = yaml_list
        self.location: Optional[Location] = get_location(yaml_list, yaml_source.file_path)

    def __iter__(self) -> iter:
        return iter([self._yaml_wrap(element) for element in self.yaml_list])

    def to_list(self) -> list:
        return self.yaml_list

    def create_location_from_yaml_list_index(self, index) -> Optional[Location]:
        if index < len(self.yaml_list):
            ruamel_location = self.yaml_list.lc.item(index)
            line: int = ruamel_location[0]
            column: int = ruamel_location[1]
            return Location(file_path=self.yaml_source.file_path, line=line, column=column)


class VariableResolver:
    @classmethod
    def resolve(
        cls,
        source_text: str,
        variables: Optional[dict[str, str]] = None,
        use_env_vars: bool = True,
        ignored_variable_names: Optional[set[str]] = None,
        location: Optional[Location] = None,
    ) -> str:
        if isinstance(source_text, str):
            return re.sub(
                pattern=r"\$\{ *(env|var)\.([a-zA-Z_][a-zA-Z_0-9]*) *\}",
                repl=lambda m: cls._resolve_variable_pattern(
                    namespace=m.group(1).strip(),
                    variable=m.group(2).strip(),
                    variables=variables,
                    use_env_vars=use_env_vars,
                    ignored_variable_names=ignored_variable_names,
                    location=location,
                ),
                string=source_text,
            )
        else:
            return source_text

    @classmethod
    def _resolve_variable_pattern(
        cls,
        namespace: str,
        variable: str,
        variables: dict[str, str],
        use_env_vars: bool,
        ignored_variable_names: Optional[set[str]] = None,
        location: Optional[Location] = None,
    ) -> str:
        value: Optional[str] = cls.get_variable(
            namespace=namespace,
            variable=variable,
            variables=variables,
            use_env_vars=use_env_vars,
            ignored_variable_names=ignored_variable_names,
            location=location,
        )
        return value if isinstance(value, str) else f"${{{namespace}.{variable}}}"

    @classmethod
    def get_variable(
        cls,
        namespace: str,
        variable: str,
        variables: Optional[dict[str, str]] = None,
        use_env_vars: bool = True,
        ignored_variable_names: Optional[set[str]] = None,
        location: Optional[Location] = None,
    ) -> Optional[str]:
        if isinstance(variables, dict) and namespace == "var":
            if isinstance(ignored_variable_names, set) and variable in ignored_variable_names:
                logger.error(
                    msg=(
                        f"Variable '{variable}' was used and provided, but not declared. "
                        f"Please add variable declaration to the contract"
                    ),
                    extra={ExtraKeys.LOCATION: location} if location else None,
                )
            return variables[variable] if isinstance(variables, dict) and variable in variables else None
        elif namespace == "env":
            if use_env_vars:
                return os.getenv(variable) if variable in os.environ else None
            else:
                logger.error(
                    msg=(
                        f"Environment variable '{variable}' will not be resolved because environment variables are "
                        f"not supported inside contract."
                    ),
                    extra={ExtraKeys.LOCATION: location} if location else None,
                )
        return None
