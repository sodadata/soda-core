from __future__ import annotations

import os
from numbers import Number

from ruamel.yaml import CommentedMap, CommentedSeq, round_trip_dump, YAML
from ruamel.yaml.error import MarkedYAMLError

from soda.contracts.impl.logs import Location, Logs
from soda.contracts.impl.variable_resolver import VariableResolver


class YamlFile:

    """
    Allows yaml configurations to be specified as a file_path, yaml_str or a yaml_dict.
    If yaml configurations are specified as text (file or str), then parsing will resolve variables.
    Then YamlFile provides access to resolved and parsed dict.
    Usage:
    variables: dict = {...}
    yaml_file: YamlFile = YamlFile(logs=logs, ...use 1 of the other arguments to specify the yaml file content...)
    yaml_file.parse(variables)
    yaml_dict: dict = yaml_file.get_dict()
    """

    def __init__(
        self,
        logs: Logs,
        yaml_file_path: str | None = None,
        yaml_str: str | None = None,
        yaml_dict: dict | None = None,
    ):
        self.file_path: str | None = yaml_file_path
        self.source_str: str | None = yaml_str
        self.resolved_str: str | None = None
        self.dict: dict | None = yaml_dict
        self.is_parsed: bool = self.file_path is None and self.source_str is None and isinstance(self.dict, dict)
        self.logs: Logs = logs

    def get_dict(self) -> dict:
        assert self.is_parsed, (
            "Usage of YamlFile requires that self.parse(...) "
            "is invoked before the dict is used."
        )
        return self.dict

    def is_ok(self):
        """
        True if the yaml dict is available and the parsing didn't have errors
        """
        return isinstance(self.dict, dict)

    def parse(self, variables: dict) -> bool:
        """
        returns is_ok() indicating that parsing was successful and the dict is available
        """
        self.is_parsed = True

        if self.file_path is None and self.source_str is None and self.dict is None:
            self.logs.error("File not configured")

        if isinstance(self.file_path, str) and self.source_str is None:
            self.source_str = self.__read_file_as_str(file_path=self.file_path, logs=self.logs)

        self.resolved_str = self.__resolve_variables(source_str=self.source_str, variables=variables, logs=self.logs)

        if isinstance(self.resolved_str, str) and self.dict is None:
            self.dict = self.__parse_yaml_str(yaml_str=self.resolved_str, logs=self.logs)

        # It is assumed that if this parse is not ok, that an error has been logged
        return self.is_ok()

    @classmethod
    def __read_file_as_str(cls, file_path: str | None, logs: Logs) -> str | None:
        try:
            with open(file_path) as f:
                return f.read()
        except OSError as e:
            if not os.path.exists(file_path):
                logs.error(f"File '{file_path}' does not exist")
            elif not os.path.isdir(file_path):
                logs.error(f"File path '{file_path}' is a directory")
            else:
                logs.error(f"File '{file_path}' can't be read: {e}")

    @classmethod
    def __resolve_variables(cls, source_str: str | None, variables: dict[str, str] | None, logs: Logs) -> str | None:
        if isinstance(source_str, str):
            # Resolve all the ${VARIABLES} in the contract based on either the provided
            # variables or system variables (os.environ)
            variable_resolver = VariableResolver(logs=logs, variables=variables)
            return variable_resolver.resolve(source_str)
        else:
            return source_str

    def __parse_yaml_str(self, yaml_str: str | None, logs: Logs) -> dict | None:
        try:
            ruamel_yaml: YAML = YAML()
            ruamel_yaml.preserve_quotes = True
            return ruamel_yaml.load(yaml_str)
        except MarkedYAMLError as e:
            mark = e.context_mark if e.context_mark else e.problem_mark
            line = mark.line + 1
            col = mark.column + 1
            location = Location(file_path=self.get_file_description(), line=line, column=col)
            logs.error(f"YAML syntax error: {e}", location)

    def get_file_description(self) -> str:
        if self.file_path:
            return self.file_path
        if self.source_str:
            return "provided YAML str"
        if self.dict:
            return "provided dict"
        return "no yaml source provided"

    def exists(self):
        if self.file_path:
            return os.path.isfile(self.file_path)
        return isinstance(self.source_str, str) or isinstance(self.dict, dict)


class YamlHelper:

    def __init__(self, logs: Logs, yaml_file: YamlFile | None = None) -> None:
        self.logs: Logs = logs
        self.yaml_file: YamlFile | None = yaml_file

    def write_to_yaml_str(self, yaml_object: object) -> str:
        try:
            return round_trip_dump(yaml_object)
        except Exception as e:
            self.logs.error(f"Couldn't write SodaCL YAML object: {e}", exception=e)

    def create_location_from_yaml_dict_key(self, d: dict, key) -> Location | None:
        if isinstance(d, CommentedMap):
            if key in d:
                ruamel_location = d.lc.value(key)
                line: int = ruamel_location[0]
                column: int = ruamel_location[1]
                return Location(file_path=self.yaml_file.get_file_description(), line=line, column=column)
            else:
                return self.create_location_from_yaml_value(d)
        return None

    def create_location_from_yaml_value(self, d: object) -> Location | None:
        if isinstance(d, CommentedMap) or isinstance(d, CommentedSeq):
            return Location(file_path=self.yaml_file.get_file_description(), line=d.lc.line, column=d.lc.col)
        return None

    def read_dict(self, d: dict, key: str) -> dict | None:
        """
        An error is generated if the value is missing or not a YAML object.
        :return: a dict if the value for the key is a YAML object, otherwise None.
        """
        return self.read_value(d=d, key=key, expected_type=dict, required=True, default_value=None)

    def read_dict_opt(self, d: dict, key: str) -> dict | None:
        """
        An error is generated if the value is present and not a YAML object.
        :return: a dict if the value for the key is a YAML object, otherwise None.
        """
        return self.read_value(d=d, key=key, expected_type=dict, required=False, default_value=None)

    def read_list(self, d: dict, key: str) -> list | None:
        """
        An error is generated if the value is missing or not a YAML list.
        :return: a list if the value for the key is a YAML list, otherwise None.
        """
        return self.read_value(d=d, key=key, expected_type=list, required=True, default_value=None)

    def read_list_opt(self, d: dict, key: str) -> list | None:
        """
        An error is generated if the value is present and not a YAML list.
        :return: a list if the value for the key is a YAML list, otherwise None.
        """
        return self.read_value(d=d, key=key, expected_type=list, required=False, default_value=None)

    def read_list_of_dicts(self, d: dict, key: str) -> list[dict] | None:
        list_value: list = self.read_list(d, key)
        if isinstance(list_value, list):
            if all(isinstance(e, dict) for e in list_value):
                return list_value
            else:
                location: Location = self.create_location_from_yaml_dict_key(d, key)
                self.logs.error(f"Not all elements in list '{key}' are objects", location=location)

    def read_list_of_strings(self, d: dict, key: str) -> list[str] | None:
        list_value = self.read_value(d=d, key=key, expected_type=list, required=True, default_value=None)
        if isinstance(list_value, list):
            if all(isinstance(e, str) for e in list_value):
                return list_value
            else:
                location: Location | None = self.create_location_from_yaml_dict_key(d, key)
                self.logs.error(message=f"Not all elements in list '{key}' are strings", location=location)

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

    def read_range(self, d: dict, key: str):  # returns Range | None
        range_yaml: list | None = self.read_list_opt(d, key)
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
                location = self.create_location_from_yaml_dict_key(d, key)
                self.logs.error(message=f"'{key}' is required", location=location)
            return default_value
        value = d.get(key)
        if expected_type is not None and not isinstance(value, expected_type):
            location = self.create_location_from_yaml_dict_key(d, key)
            self.logs.error(
                message=f"'{key}' expected a {expected_type.__name__}, but was {type(value).__name__}",
                location=location,
            )
        return value


class QuotingSerializer:

    @classmethod
    def quote(cls, name: str) -> str:
        return (
            f'"{name}"'
            # Depends on ruamel class names DoubleQuotedScalarString and SingleQuotedScalarString
            if isinstance(name, str) and "Quoted" in type(name).__name__
            else name
        )
