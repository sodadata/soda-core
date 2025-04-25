from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from numbers import Number
from typing import Optional

from soda_core.common.current_time import CurrentTime
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.datetime_conversions import (
    convert_datetime_to_str,
    convert_str_to_datetime,
)
from soda_core.common.logging_constants import Emoticons, ExtraKeys, soda_logger
from soda_core.common.logs import Location
from soda_core.common.yaml import (
    ContractYamlSource,
    VariableResolver,
    YamlList,
    YamlObject,
    YamlValue,
)

logger: logging.Logger = soda_logger


def register_check_types() -> None:
    from soda_core.contracts.impl.check_types.schema_check_yaml import (
        SchemaCheckYamlParser,
    )
    from soda_core.contracts.impl.contract_verification_impl import CheckImpl

    CheckYaml.register(SchemaCheckYamlParser())
    from soda_core.contracts.impl.check_types.schema_check import SchemaCheckParser

    CheckImpl.register(SchemaCheckParser())

    from soda_core.contracts.impl.check_types.missing_check_yaml import (
        MissingCheckYamlParser,
    )

    CheckYaml.register(MissingCheckYamlParser())
    from soda_core.contracts.impl.check_types.missing_check import MissingCheckParser

    CheckImpl.register(MissingCheckParser())

    from soda_core.contracts.impl.check_types.invalidity_check_yaml import (
        InvalidCheckYamlParser,
    )

    CheckYaml.register(InvalidCheckYamlParser())
    from soda_core.contracts.impl.check_types.invalidity_check import InvalidCheckParser

    CheckImpl.register(InvalidCheckParser())

    from soda_core.contracts.impl.check_types.row_count_check_yaml import (
        RowCountCheckYamlParser,
    )

    CheckYaml.register(RowCountCheckYamlParser())
    from soda_core.contracts.impl.check_types.row_count_check import RowCountCheckParser

    CheckImpl.register(RowCountCheckParser())


class ContractYaml:
    """
    Represents YAML as close as possible.
    None means the key was not present.
    If property value types do not match the schema, None value will be in the model
    List properties will have a None value if the property is not present or the content was not a list, a list otherwise
    """

    @classmethod
    def parse(
        cls,
        contract_yaml_source: ContractYamlSource,
        provided_variable_values: Optional[dict[str, str]] = None,
    ) -> Optional[ContractYaml]:
        check_types_have_been_registered: bool = len(CheckYaml.check_yaml_parsers) > 0
        if not check_types_have_been_registered:
            register_check_types()
        return ContractYaml(
            contract_yaml_source=contract_yaml_source,
            provided_variable_values=provided_variable_values,
        )

    def __init__(
        self,
        contract_yaml_source: ContractYamlSource,
        provided_variable_values: Optional[dict[str, str]],
    ):
        self.contract_yaml_source: ContractYamlSource = contract_yaml_source
        self.contract_yaml_object: Optional[YamlObject] = contract_yaml_source.parse()

        self.variables: list[VariableYaml] = self._parse_variable_yamls(contract_yaml_source, provided_variable_values)

        self.data_timestamp: datetime = CurrentTime.now()
        soda_variable_values: dict[str, str] = {"NOW": convert_datetime_to_str(self.data_timestamp)}

        self.resolved_variable_values: dict[str, str] = self._resolve_variable_values(
            variable_yamls=self.variables,
            provided_variable_values=provided_variable_values,
            soda_variable_values=soda_variable_values,
        )

        if "NOW" in self.resolved_variable_values:
            now_value = self.resolved_variable_values.get("NOW")
            if convert_str_to_datetime(now_value) is None:
                logger.error(f"Variable 'NOW' must be a correct ISO 8601 timestamp format: {now_value}")

        self.contract_yaml_source.resolve_on_read_value(
            resolved_variable_values=self.resolved_variable_values, soda_values=soda_variable_values, use_env_vars=True
        )

        if (
            self.contract_yaml_object
            and self.contract_yaml_object.has_key("datasource")
            and not self.contract_yaml_object.has_key("data_source")
        ):
            logger.error(
                msg="Key `datasource` must be 2 words. " "Please change to `data_source`.",
                extra={
                    ExtraKeys.LOCATION: self.contract_yaml_object.create_location_from_yaml_dict_key("datasource"),
                },
            )

        self.dataset: Optional[str] = (
            self.contract_yaml_object.read_string("dataset") if self.contract_yaml_object else None
        )
        self.filter: Optional[str] = (
            self.contract_yaml_object.read_string_opt("filter") if self.contract_yaml_object else None
        )
        if not self.filter:
            self.filter = (
                self.contract_yaml_object.read_string_opt("checks_filter") if self.contract_yaml_object else None
            )
        if self.filter:
            self.filter = self.filter.strip()

        # Validate qualified dataset name
        _ = DatasetIdentifier.parse(self.dataset)

        self.columns: Optional[list[Optional[ColumnYaml]]] = self._parse_columns(self.contract_yaml_object)
        self.checks: Optional[list[Optional[CheckYaml]]] = self._parse_checks(self.contract_yaml_object)

    def _parse_variable_yamls(self, contract_yaml_source, variables) -> list[VariableYaml]:
        variable_yamls: list[VariableYaml] = []

        if self.contract_yaml_object:
            variables_yaml_object: Optional[YamlObject] = self.contract_yaml_object.read_object_opt("variables")
            if variables_yaml_object:
                for variable_name, variable_yaml_object in variables_yaml_object.items():
                    variable_yaml: VariableYaml = VariableYaml(variable_name, variable_yaml_object)
                    variable_yamls.append(variable_yaml)

        return variable_yamls

    def _resolve_variable_values(
        self,
        variable_yamls: list[VariableYaml],
        provided_variable_values: Optional[dict[str, str]],
        soda_variable_values: Optional[dict[str, str]],
    ) -> dict[str, str]:
        variable_values: dict[str, str] = {}

        # Initializing the declared variables
        for variable_yaml in variable_yamls:
            variable_name: str = variable_yaml.name
            variable_values[variable_name] = (
                provided_variable_values.get(variable_name)
                if isinstance(provided_variable_values, dict) and variable_name in provided_variable_values
                else variable_yaml.default
            )

        for variable_yaml in variable_yamls:
            if variable_values.get(variable_yaml.name) is None:
                logger.error(f"Required variable '{variable_yaml.name}' did not get a value")

        if "NOW" in provided_variable_values:
            now_str: str = provided_variable_values["NOW"]
            if not isinstance(now_str, str):
                logger.error(f"Provided 'NOW' variable must be a string, but was: {now_str.__class__.__name__}")
            else:
                if convert_str_to_datetime(now_str) is None:
                    logger.error(f"Provided 'NOW' variable value is not a correct ISO 8601 timestamp format: {now_str}")
                variable_values["NOW"] = now_str
        else:
            # Default now initialization
            variable_values["NOW"] = convert_datetime_to_str(datetime.now())

        # Checking variable values against their declared type & required
        for variable_yaml in variable_yamls:
            if variable_yaml.required and variable_values.get(variable_yaml.name) is None:
                logger.error(
                    msg=f"Required variable '{variable_yaml.name}' not provided",
                    extra={ExtraKeys.LOCATION: variable_yaml.variable_yaml_object.location},
                )
            elif variable_yaml.type == "timestamp":
                resolved_timestamp_value = variable_values.get(variable_yaml.name)
                if resolved_timestamp_value is not None and convert_str_to_datetime(resolved_timestamp_value) is None:
                    logger.error(
                        msg=f"Invalid timestamp value for variable '{variable_yaml.name}': "
                        f"{resolved_timestamp_value}",
                        extra={ExtraKeys.LOCATION: variable_yaml.variable_yaml_object.location},
                    )

        return self._resolve_variables(variable_values=variable_values, soda_variable_values=soda_variable_values)

    @classmethod
    def _resolve_variables(
        cls, variable_values: Optional[dict[str, str]], soda_variable_values: Optional[dict[str, str]]
    ) -> dict[str, str]:
        """
        Resolve all variables in the dictionary, replacing ${variable_name} expressions
        with their corresponding values, while detecting circular dependencies.

        Args:
            variable_values (dict): Dictionary with string keys and string values
                             containing ${variable_name} expressions

        Returns:
            dict: Dictionary with all variables resolved
        """
        # Create a copy of the input dict to avoid modifying the original
        variable_values = variable_values.copy()

        # Keep track of variables being processed to detect circular references
        processing_stack = set()

        def resolve_value(name: str, value: str) -> str:
            """
            Recursively resolve all variables in a given value.

            Args:
                name (str): The variable name being resolved (for circular detection)
                value (str): The value to resolve

            Returns:
                str: The resolved value
            """
            # Check for circular reference
            if name in processing_stack:
                # Circular reference detected - return the original expression
                # to prevent infinite recursion
                return value

            # Add current variable to the processing stack
            processing_stack.add(name)

            # Replace all variable references in the value
            resolved = VariableResolver.resolve(
                source_text=value,
                variable_values=variable_values,
                soda_variable_values=soda_variable_values,
                use_env_vars=False,
            )

            # Remove current variable from the processing stack
            processing_stack.remove(name)

            return resolved

        # Resolve each variable in the dictionary
        for name in variable_values:
            variable_values[name] = resolve_value(name, variable_values[name])

        for name, value in variable_values.items():
            logger.debug(f"var.{name} = {value}")

        return variable_values

    def _parse_columns(self, contract_yaml_object: YamlObject) -> Optional[list[Optional[ColumnYaml]]]:
        columns: Optional[list[Optional[ColumnYaml]]] = None
        if contract_yaml_object:
            column_yaml_objects: Optional[YamlList] = contract_yaml_object.read_list_of_objects_opt("columns")
            if isinstance(column_yaml_objects, YamlList):
                columns = []
                column_locations_by_name: dict[str, list[Optional[Location]]] = {}
                for column_yaml_object in column_yaml_objects:
                    if isinstance(column_yaml_object, YamlObject):
                        column_yaml: ColumnYaml = ColumnYaml(contract_yaml=self, column_yaml_object=column_yaml_object)
                        columns.append(column_yaml)
                        if isinstance(column_yaml.name, str):
                            (
                                column_locations_by_name.setdefault(column_yaml.name, []).append(
                                    column_yaml_object.location
                                )
                            )
                for column_name, locations in column_locations_by_name.items():
                    if len(locations) > 1:
                        locations_message: str = ", ".join(
                            [f"[{location.line},{location.column}]" for location in locations if location is not None]
                        )
                        file_location = (
                            f"In {self.contract_yaml_source.file_path} at: "
                            if self.contract_yaml_source.file_path
                            else "At file locations: "
                        )
                        locations_message = f": {file_location}{locations_message}" if locations_message else ""
                        logger.error(f"Duplicate columns with name " f"'{column_name}'{locations_message}")
        return columns

    def _parse_checks(
        self, checks_containing_yaml_object: YamlObject, column_yaml: Optional[ColumnYaml] = None
    ) -> Optional[list[Optional[CheckYaml]]]:
        checks: Optional[list[Optional[CheckYaml]]] = None

        if checks_containing_yaml_object:
            checks_yaml_list: YamlList = checks_containing_yaml_object.read_list_opt("checks")
            if checks_yaml_list:
                checks = []
                for check_index, check_yaml_object in enumerate(checks_yaml_list):
                    check_type_name: Optional[str] = None
                    check_body_yaml_object: Optional[YamlObject] = None

                    if isinstance(check_yaml_object, YamlObject):
                        check_keys: list[str] = check_yaml_object.keys()
                        if len(check_keys) != 1:
                            logging.error(
                                msg=f"Checks require 1 key to be the type",
                                extra={
                                    ExtraKeys.LOCATION: check_yaml_object.location,
                                },
                            )
                        else:
                            check_type_name = check_keys.pop()
                            check_body_yaml_object = check_yaml_object.read_object_opt(key=check_type_name)
                    elif isinstance(check_yaml_object, str):
                        check_type_name = check_yaml_object
                        logger.error(
                            f"{Emoticons.PINCHED_FINGERS} Mama Mia! You forgot the "
                            f"colon ':' behind the check '{check_type_name}'."
                        )
                    if isinstance(check_type_name, str):
                        if check_body_yaml_object is None:
                            check_body_yaml_object = YamlObject(
                                yaml_source=checks_containing_yaml_object.yaml_source, yaml_dict={}
                            )
                            check_body_yaml_object.location = checks_yaml_list.create_location_from_yaml_list_index(
                                index=check_index
                            )

                        check_yaml: Optional[CheckYaml] = CheckYaml.parse_check_yaml(
                            check_type_name=check_type_name,
                            check_body_yaml_object=check_body_yaml_object,
                            column_yaml=column_yaml,
                        )
                        if check_yaml:
                            checks.append(check_yaml)
                        else:
                            logger.error(
                                f"Invalid check type '{check_type_name}'. "
                                f"Existing check types: {CheckYaml.get_check_type_names()}"
                            )
                    else:
                        logger.error(f"Checks must have a YAML object structure.")

        return checks


class VariableYaml:
    def __init__(self, variable_name: str, variable_yaml_object: YamlObject):
        self.variable_yaml_object: YamlObject = variable_yaml_object
        self.name: str = variable_name
        self.type: any = variable_yaml_object.read_string_opt("type") if variable_yaml_object else None
        self.required: any = variable_yaml_object.read_bool_opt("required") if variable_yaml_object else None
        self.default: any = variable_yaml_object.read_string_opt("default") if variable_yaml_object else None


class ValidReferenceDataYaml:
    def __init__(self, valid_reference_data_yaml: YamlObject):
        dataset: any = valid_reference_data_yaml.read_value("dataset")
        is_list_str: bool = isinstance(dataset, list) and all(isinstance(e, str) for e in dataset)
        self.dataset: str | list[str] | None = dataset if isinstance(dataset, str) or is_list_str else None
        self.column: Optional[str] = valid_reference_data_yaml.read_string("column")

        cfg_keys = valid_reference_data_yaml.yaml_dict.keys()
        self.has_configuration_error: bool = ("dataset" in cfg_keys and self.dataset is None) and (
            "column" in cfg_keys and self.column is None
        )

        if self.dataset is None:
            self.has_configuration_error = True
            logger.error(
                msg=(
                    f"'dataset' is required. Must be the dataset name as a string "
                    "or a list of strings representing the qualified name."
                ),
                extra={
                    ExtraKeys.LOCATION: valid_reference_data_yaml.location,
                },
            )


@dataclass
class RegexFormat:
    regex: str
    name: Optional[str]

    @classmethod
    def read(cls, yaml_object: YamlObject, key: str) -> Optional[RegexFormat]:
        regex_format_yaml_object: Optional[YamlObject] = yaml_object.read_object_opt(key)
        if regex_format_yaml_object:
            regex: Optional[str] = regex_format_yaml_object.read_string("regex")
            name: Optional[str] = regex_format_yaml_object.read_string("name")
            if isinstance(regex, str):
                return RegexFormat(regex=regex, name=name)
        return None


class MissingAndValidityYaml:
    def __init__(self, yaml_object: YamlObject):
        self.missing_values: Optional[list] = YamlValue.yaml_unwrap(yaml_object.read_list_opt("missing_values"))
        self.missing_format: Optional[RegexFormat] = RegexFormat.read(yaml_object=yaml_object, key="missing_format")

        # cfg_keys = yaml_object.yaml_dict.keys()
        # self.has_missing_configuration_error: bool = ("missing_values" in cfg_keys and self.missing_values is None) or (
        #     "missing_regex_sql" in cfg_keys and self.missing_regex is None
        # )

        self.invalid_values: Optional[list] = YamlValue.yaml_unwrap(yaml_object.read_list_opt("invalid_values"))
        self.invalid_format: Optional[RegexFormat] = RegexFormat.read(yaml_object=yaml_object, key="invalid_format")
        self.valid_values: Optional[list] = YamlValue.yaml_unwrap(yaml_object.read_list_opt("valid_values"))
        self.valid_format: Optional[RegexFormat] = RegexFormat.read(yaml_object=yaml_object, key="valid_format")
        self.valid_min: Optional[Number] = yaml_object.read_number_opt("valid_min")
        self.valid_max: Optional[Number] = yaml_object.read_number_opt("valid_max")
        self.valid_length: Optional[int] = yaml_object.read_number_opt("valid_length")
        self.valid_min_length: Optional[int] = yaml_object.read_number_opt("valid_min_length")
        self.valid_max_length: Optional[int] = yaml_object.read_number_opt("valid_max_length")

        self.valid_reference_data: Optional[ValidReferenceDataYaml] = None
        valid_reference_data_yaml: Optional[YamlObject] = yaml_object.read_object_opt("valid_reference_data")
        if valid_reference_data_yaml:
            self.valid_reference_data = ValidReferenceDataYaml(valid_reference_data_yaml)

        # self.has_valid_configuration_error: bool = (
        #     ("invalid_values" in cfg_keys and self.invalid_values is None)
        #     or ("invalid_format" in cfg_keys and self.invalid_format is None)
        #     or ("valid_values" in cfg_keys and self.valid_values is None)
        #     or ("valid_format" in cfg_keys and self.valid_format is None)
        #     or ("valid_min" in cfg_keys and self.valid_min is None)
        #     or ("valid_max" in cfg_keys and self.valid_max is None)
        #     or ("valid_length" in cfg_keys and self.valid_length is None)
        #     or ("valid_min_length" in cfg_keys and self.valid_min_length is None)
        #     or ("valid_max_length" in cfg_keys and self.valid_max_length is None)
        #     or ("valid_reference_data" in cfg_keys and self.valid_reference_data.has_configuration_error)
        # )


class ColumnYaml(MissingAndValidityYaml):
    def __init__(self, contract_yaml: ContractYaml, column_yaml_object: YamlObject):
        self.column_yaml_object: YamlObject = column_yaml_object
        self.name: Optional[str] = column_yaml_object.read_string("name")
        self.data_type: Optional[str] = column_yaml_object.read_string_opt("data_type")
        self.character_maximum_length: Optional[int] = column_yaml_object.read_number_opt("character_maximum_length")
        super().__init__(column_yaml_object)
        self.check_yamls: Optional[list[CheckYaml]] = contract_yaml._parse_checks(
            checks_containing_yaml_object=column_yaml_object, column_yaml=self
        )


class RangeYaml:
    """
    Boundary values are inclusive
    """

    def __init__(self, lower_bound: Number, upper_bound: Number):
        self.lower_bound: Number = lower_bound
        self.upper_bound: Number = upper_bound

    @classmethod
    def read_opt(cls, check_yaml_object: YamlObject, key: str) -> Optional[RangeYaml]:
        range_yaml_list: YamlList = check_yaml_object.read_list_opt(key)
        if range_yaml_list:
            lower_bound: Optional[Number] = None
            upper_bound: Optional[Number] = None
            range_list: list = range_yaml_list.to_list()
            if len(range_list) > 0 and isinstance(range_list[0], Number):
                lower_bound = range_list[0]
            if len(range_list) > 1 and isinstance(range_list[1], Number):
                upper_bound = range_list[1]
            return RangeYaml(lower_bound=lower_bound, upper_bound=upper_bound)


class CheckYamlParser(ABC):
    @abstractmethod
    def get_check_type_names(self) -> list[str]:
        pass

    @abstractmethod
    def parse_check_yaml(
        self, check_type_name: str, check_yaml_object: YamlObject, column_yaml: Optional[ColumnYaml]
    ) -> Optional[CheckYaml]:
        pass


class CheckYaml(ABC):
    check_yaml_parsers: dict[str, CheckYamlParser] = {}

    @classmethod
    def register(cls, check_yaml_parser: CheckYamlParser) -> None:
        for check_type_name in check_yaml_parser.get_check_type_names():
            cls.check_yaml_parsers[check_type_name] = check_yaml_parser

    @classmethod
    def get_check_type_names(cls) -> list[str]:
        return list(cls.check_yaml_parsers.keys())

    @classmethod
    def parse_check_yaml(
        cls, check_type_name: str, check_body_yaml_object: YamlObject, column_yaml: Optional[ColumnYaml]
    ) -> Optional[CheckYaml]:
        if isinstance(check_type_name, str):
            check_yaml_parser: Optional[CheckYamlParser] = cls.check_yaml_parsers.get(check_type_name)
            if check_yaml_parser:
                return check_yaml_parser.parse_check_yaml(
                    check_type_name=check_type_name,
                    check_yaml_object=check_body_yaml_object,
                    column_yaml=column_yaml,
                )

    def __init__(self, type_name: str, check_yaml_object: YamlObject):
        self.check_yaml_object: YamlObject = check_yaml_object
        self.type_name: str = type_name
        self.name: Optional[str] = check_yaml_object.read_string_opt("name") if check_yaml_object else None
        qualifier = check_yaml_object.read_value("qualifier") if check_yaml_object else None
        self.qualifier: Optional[str] = str(qualifier) if qualifier is not None else None
        self.filter: Optional[str] = check_yaml_object.read_string_opt("filter") if check_yaml_object else None
        if self.filter:
            self.filter = self.filter.strip()


class ThresholdCheckYaml(CheckYaml):
    def __init__(self, type_name: str, check_yaml_object: YamlObject):
        super().__init__(type_name=type_name, check_yaml_object=check_yaml_object)
        self.metric: Optional[str] = check_yaml_object.read_string_opt("metric")
        self.threshold: Optional[ThresholdYaml] = None
        threshold_yaml_object: YamlObject = check_yaml_object.read_object_opt("threshold")
        if threshold_yaml_object:
            self.threshold = ThresholdYaml(threshold_yaml_object)


class ThresholdYaml:
    def __init__(self, threshold_yaml_object: YamlObject):
        self.must_be_greater_than: Optional[Number] = threshold_yaml_object.read_number_opt("must_be_greater_than")
        self.must_be_greater_than_or_equal: Optional[Number] = threshold_yaml_object.read_number_opt(
            "must_be_greater_than_or_equal"
        )
        self.must_be_less_than: Optional[Number] = threshold_yaml_object.read_number_opt("must_be_less_than")
        self.must_be_less_than_or_equal: Optional[Number] = threshold_yaml_object.read_number_opt(
            "must_be_less_than_or_equal"
        )
        self.must_be: Optional[Number] = threshold_yaml_object.read_number_opt("must_be")
        self.must_not_be: Optional[Number] = threshold_yaml_object.read_number_opt("must_not_be")
        self.must_be_between: Optional[RangeYaml] = RangeYaml.read_opt(threshold_yaml_object, "must_be_between")
        self.must_be_not_between: Optional[RangeYaml] = RangeYaml.read_opt(threshold_yaml_object, "must_be_not_between")


class MissingAncValidityCheckYaml(ThresholdCheckYaml, MissingAndValidityYaml):
    def __init__(self, type_name: str, check_yaml_object: YamlObject):
        ThresholdCheckYaml.__init__(self, type_name=type_name, check_yaml_object=check_yaml_object)
        MissingAndValidityYaml.__init__(self, yaml_object=check_yaml_object)
