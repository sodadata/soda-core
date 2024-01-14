from __future__ import annotations

import logging
from numbers import Number
from typing import List, Dict

from ruamel.yaml import CommentedMap

from soda.contracts.connection import SodaException
from soda.contracts.contract import Contract, Check, SchemaCheck, NumericThreshold, MissingConfigurations, \
    NumericMetricCheck, ValidReferenceColumn, ValidConfigurations, Range
from soda.contracts.impl.json_schema_verifier import JsonSchemaVerifier
from soda.contracts.impl.logs import Logs
from soda.contracts.impl.variable_resolver import VariableResolver
from soda.contracts.impl.yaml import YamlList, YamlObject, YamlParser, YamlWrapper

logger = logging.getLogger(__name__)


class ContractParser:

    def __init__(self, logs: Logs | None = None):
        super().__init__()
        self.logs = logs if logs else Logs()
        self.missing_value_configs_by_column = {}
        self.skip_schema_validation: bool = False

    def parse_contract(self,
                       contract_yaml_str: str,
                       variables: dict[str, str] | None = None
                       ) -> Contract:
        if not isinstance(contract_yaml_str, str):
            raise SodaException(f"contract_yaml_str must be a str, but was {type(contract_yaml_str)}")

        # Resolve all the ${VARIABLES} in the contract based on either the provided
        # variables or system variables (os.environ)
        variable_resolver = VariableResolver(logs=self.logs, variables=variables)
        resolved_contract_yaml_str: str = variable_resolver.resolve(contract_yaml_str)

        yaml_parser: YamlParser = YamlParser(logs=self.logs)
        ruamel_yaml_object: object | None = yaml_parser.parse_yaml_str(yaml_str=resolved_contract_yaml_str)

        if not isinstance(ruamel_yaml_object, CommentedMap):
            raise SodaException("The contract top level must be a YAML object with keys and values")

        # Verify the contract schema on the ruamel instance object
        if ruamel_yaml_object is not None:
            json_schema_verifier: JsonSchemaVerifier = JsonSchemaVerifier(self.logs)
            json_schema_verifier.verify(ruamel_yaml_object)

        # Wrap the ruamel_yaml_object into the YamlValue (this is a better API for writing the translator)
        yaml_wrapper: YamlWrapper = YamlWrapper(logs=self.logs)
        contract_yaml_object: YamlObject = yaml_wrapper.wrap(ruamel_yaml_object)

        dataset_name: str | None = contract_yaml_object.read_string("dataset")
        schema_name: str | None = contract_yaml_object.read_string_opt("schema")

        checks: list[Check] = []

        contract_columns_yaml_list: YamlList | None = contract_yaml_object.read_yaml_list("columns")
        if contract_columns_yaml_list:
            schema_columns: Dict[str,str | None] = {}
            schema_optional_columns: List[str] = []

            checks.append(SchemaCheck(
                metric="schema",
                name="Schema",
                contract_check_id=str(len(checks)),
                location=None,
                columns=schema_columns,
                optional_columns=schema_optional_columns
            ))

            for contract_column_yaml_list_element in contract_columns_yaml_list:
                contract_column_yaml_object: YamlObject = contract_column_yaml_list_element
                column_name: str = contract_column_yaml_object.read_string("name")

                data_type: str | None = contract_column_yaml_object.read_string_opt("data_type")
                schema_columns[column_name] = data_type

                if contract_column_yaml_object.read_bool_opt("optional", default_value=False):
                    schema_optional_columns.append(column_name)

                yaml_column_checks: YamlList = contract_column_yaml_object.read_yaml_list_opt("checks")

                if yaml_column_checks:
                    for yaml_column_check in yaml_column_checks:
                        check: Check = self._parse_column_check(
                            contract_check_id=str(len(checks)),
                            check_yaml=yaml_column_check,
                            column=column_name
                        )
                        if check:
                            checks.append(check)
                        else:
                            logging.error(f"Could not parse check for {yaml_column_check.unpacked()}")

        checks_yaml_list: YamlList | None = contract_yaml_object.read_yaml_list_opt("checks")
        if checks_yaml_list:
            for check_yaml_object in checks_yaml_list:
                column_name: str = check_yaml_object.read_string_opt("column")
                check = self._parse_dataset_check(check=check_yaml_object, column_name=column_name)
                if check:
                    checks.append(check)

        return Contract(
            dataset=dataset_name,
            schema=schema_name,
            checks=checks,
            contract_yaml_str=contract_yaml_str
        )

    def _parse_column_check(self,
                            contract_check_id: str,
                            check_yaml: YamlObject,
                            check_type: str | None,
                            column: str,
                            default_threshold: NumericThreshold | None
                            ) -> Check | None:

        check_type = check_yaml.read_string("type")
        if check_type is None:
            return None

        if check_type in ["missing", "not_null"]:
            check_type = "missing_count"
        if check_type == "invalid":
            check_type = "invalid_count"

        default_threshold: NumericThreshold | None = (
            NumericThreshold(not_equals=0) if check_type in[
                "missing_count", "missing_percent", "invalid_count", "invalid_percent",
                "duplicate_count", "duplicate_count"
            ]
            else None
        )

        metric = f"{check_type}({column})"

        return self._parse_numeric_metric_check(
            contract_check_id=contract_check_id,
            check_yaml=check_yaml,
            metric=metric,
            column=column,
            default_threshold=default_threshold
        )

    def _parse_numeric_metric_check(self,
                                    contract_check_id: str,
                                    check_yaml: YamlObject,
                                    metric: str,
                                    column: str | None,
                                    default_threshold: NumericThreshold | None
                                    ) -> Check | None:

        name = check_yaml.read_yaml_string_opt("name")
        fail_threshold: NumericThreshold = self._parse_numeric_threshold(
            check_yaml=check_yaml,
            prefix="fail_when_",
            default_threshold=default_threshold
        )

        for k in check_yaml:
            if k.startswith("warn_when"):
                self.logs.error(message=f"Warnings not yet supported: '{k}'", location=check_yaml.location)

        missing_configurations: MissingConfigurations | None = self._parse_missing_configurations(
            check_yaml=check_yaml
        )
        if missing_configurations:
            # If a missing config is specified, do a complete overwrite.
            # Overwriting the missing configs gives more control to the contract author over merging the missing configs.
            self.missing_value_configs_by_column[column] = missing_configurations
        elif column is not None:
            missing_configurations = self.missing_value_configs_by_column.get(column)

        valid_configurations: ValidConfigurations | None = self._parse_valid_configurations(
            check_yaml=check_yaml
        )

        other_check_configs: dict = {
            k: v for k, v in check_yaml.unpacked().items()
            if k not in ["type", "name", "fail_when_greater_than", "fail_when_greater_than_or_equal",
                         "fail_when_less_than", "fail_when_less_than_or_equal", "fail_when_equals",
                         "fail_when_not_equals", "fail_when_between", "fail_when_not_between", "missing_values",
                         "missing_regex", "invalid_values", "invalid_format", "invalid_regex", "valid_values",
                         "valid_format", "valid_regex", "valid_min", "valid_max", "valid_length", "valid_min_length",
                         "valid_max_length", "valid_reference_column"
                         ]
        }

        return NumericMetricCheck(
            metric=metric,
            name=name,
            contract_check_id=contract_check_id,
            location=check_yaml.location,
            column=column,
            missing_configurations=missing_configurations,
            valid_configurations=valid_configurations,
            fail_threshold=fail_threshold,
            warn_threshold=None,
            other_check_configs=other_check_configs
        )

    def _parse_missing_configurations(self, check_yaml: YamlObject) -> MissingConfigurations | None:
        missing_values_yaml_list: YamlList | None = check_yaml.read_yaml_list_opt(f"missing_values")
        missing_values: list | None = missing_values_yaml_list.unpacked() if missing_values_yaml_list else None
        missing_regex: str | None = check_yaml.read_string_opt(f"missing_regex")

        if all(v is None for v in [missing_values, missing_regex]):
            return None
        else:
            return MissingConfigurations(
                missing_values=missing_values,
                missing_regex=missing_regex
            )

    def _parse_valid_configurations(self, check_yaml: YamlObject) -> ValidConfigurations | None:
        invalid_values_yaml_list: YamlList | None = check_yaml.read_yaml_list_opt(f"invalid_values")
        invalid_values: list | None = invalid_values_yaml_list.unpacked() if invalid_values_yaml_list else None
        invalid_format: str | None = check_yaml.read_string_opt(f"invalid_format")
        invalid_regex: str | None = check_yaml.read_string_opt(f"invalid_regex")

        valid_values_yaml_list: YamlList | None = check_yaml.read_yaml_list_opt(f"valid_values")
        valid_values: list | None = valid_values_yaml_list.unpacked() if valid_values_yaml_list else None

        valid_format: str | None = check_yaml.read_string_opt(f"valid_format")
        valid_regex: str | None = check_yaml.read_string_opt(f"valid_regex")

        valid_min: Number | None = check_yaml.read_number_opt(f"valid_min")
        valid_max: Number | None = check_yaml.read_number_opt(f"valid_max")

        valid_length: int | None = check_yaml.read_number_opt(f"valid_length")
        valid_min_length: int | None = check_yaml.read_number_opt(f"valid_min_length")
        valid_max_length: int | None = check_yaml.read_number_opt(f"valid_max_length")

        valid_reference_column: ValidReferenceColumn | None = None
        valid_reference_column_yaml_object: YamlObject | None = check_yaml.read_yaml_object_opt(f"valid_reference_column")
        if valid_reference_column_yaml_object:
            ref_dataset = valid_reference_column_yaml_object.read_string("dataset")
            ref_column = valid_reference_column_yaml_object.read_string("column")
            valid_reference_column = ValidReferenceColumn(dataset=ref_dataset, column=ref_column)

        if all(v is None for v in [invalid_values, invalid_format, invalid_regex, valid_values, valid_format,
                                   valid_regex, valid_min, valid_max, valid_length, valid_min_length,
                                   valid_max_length, valid_reference_column]):
            return None
        else:
            return ValidConfigurations(
                invalid_values=invalid_values,
                invalid_format=invalid_format,
                invalid_regex=invalid_regex,
                valid_values = valid_values,
                valid_format=valid_format,
                valid_regex=valid_regex,
                valid_min=valid_min,
                valid_max=valid_max,
                valid_length=valid_length,
                valid_min_length=valid_min_length,
                valid_max_length=valid_max_length,
                valid_reference_column = valid_reference_column
            )

    def _parse_numeric_threshold(self,
                                 check_yaml: YamlObject,
                                 prefix: str,
                                 default_threshold: NumericThreshold | None
                                 ) -> NumericThreshold | None:
        greater_than = check_yaml.read_number_opt(f"{prefix}greater_than")
        greater_than_or_equal = check_yaml.read_number_opt(f"{prefix}greater_than_or_equal")
        less_than = check_yaml.read_number_opt(f"{prefix}less_than")
        less_than_or_equal = check_yaml.read_number_opt(f"{prefix}less_than_or_equal")
        equals = check_yaml.read_number_opt(f"{prefix}equals")
        not_equals = check_yaml.read_number_opt(f"{prefix}not_equals")
        between = self._parse_range(check_yaml, f"{prefix}between")
        not_between = self._parse_range(check_yaml, f"{prefix}not_between")

        if all(v is None for v in [greater_than, greater_than_or_equal, less_than, less_than_or_equal, equals,
                                   not_equals, between, not_between]):
            return default_threshold
        else:
            return NumericThreshold(
                greater_than=greater_than,
                greater_than_or_equal=greater_than_or_equal,
                less_than=less_than,
                less_than_or_equal=less_than_or_equal,
                equals=equals,
                not_equals=not_equals,
                between=between,
                not_between=not_between
            )

    def _parse_range(self, check_yaml: YamlObject, range_key: str) -> Range | None:
        range_yaml_list: YamlList | None = check_yaml.read_yaml_list_opt(range_key)
        if isinstance(range_yaml_list, YamlList):
            range_values = range_yaml_list.unpacked()
            if (
                all(isinstance(range_value, Number) for range_value in range_values)
                and len(range_values) == 2
            ):
                return Range(lower_bound=range_values[0], upper_bound=range_values[1])
            else:
                self.logs.error("range expects a list of 2 numbers", location=range_yaml_list.location)

    def has_errors(self) -> bool:
        return self.logs.has_errors()

    #     elif check_type in ["invalid", "invalid_count", "invalid_percent"]:
    #         return self._parse_column_invalid_check(check=yaml_column_check, check_type=check_type, column_name=column_name)
    #
    #     elif check_type in ["unique", "duplicate_count"]:
    #         return self._parse_column_duplicates_check(check=yaml_column_check, check_type=check_type, column_name=column_name)
    #
    #     else:
    #         return self._parse_column_aggregation_check(check=yaml_column_check, check_type=check_type, column_name=column_name)
    #
    # def _parse_column_missing_check(self,
    #                                 contract_check_id: str,
    #                                 yaml_check: YamlObject,
    #                                 check_type: str,
    #                                 column_name: str
    #                                 ) -> Check | None:
    #     if check_type in ["missing", "not_null"]:
    #         check_type = "missing_count"
    #     metric = f"{check_type}({column_name})"
    #     return self._create_numeric_metric_check(
    #         yaml_check=yaml_check,
    #         contract_check_id=contract_check_id,
    #         metric=metric,
    #     )
    #
    # def _parse_column_invalid_check(self,
    #                                 contract_check_id: str,
    #                                 yaml_check: YamlObject,
    #                                 check_type: str,
    #                                 column_name: str
    #                                 ) -> Check | None:
    #     if check_type == "invalid":
    #         check_type = "invalid_count"
    #     metric = f"{check_type}({column_name})"
    #     return self._create_numeric_metric_check(
    #         yaml_check=yaml_check,
    #         contract_check_id=contract_check_id,
    #         metric=metric,
    #     )
    #
    #
    #     check_configs: dict = self._parse_check_configs(check)
    #
    #     missing_configs: dict | None = self.missing_value_configs_by_column.get(column_name)
    #     if missing_configs:
    #         new_check_configs = copy.deepcopy(missing_configs)
    #         new_check_configs.update(check_configs)
    #         check_configs = new_check_configs
    #
    #     valid_values_column: YamlObject = check.read_yaml_object_opt("valid_values_column")
    #     if valid_values_column is not None:
    #         if not isinstance(valid_values_column, YamlObject):
    #             self.error(
    #                 f"Expected object for contents of 'valid_values_column', "
    #                 f"but was {type(valid_values_column)}, {check.location}"
    #             )
    #         else:
    #             ref_dataset: str | None = valid_values_column.read_string("dataset")
    #             ref_column: str | None = valid_values_column.read_string("column")
    #             if not ref_dataset or not ref_column:
    #                 logger.error(f"Reference check must have 'dataset' and 'column'. {check.location}")
    #                 return None
    #             reference_check_line = (
    #                 f"values in ({column_name}) must exist in {ref_dataset} ({ref_column})"
    #             )
    #             check_configs.pop("valid values column")
    #             return self._create_check(reference_check_line, check_configs)
    #
    #     if check_type == "invalid":
    #         check_type = f"invalid_count"
    #
    #     metric = f"{check_type}({column_name})"
    #     return self._create_numeric_metric_check(metric, check)
    #
    # def _parse_column_duplicates_check(self, check: YamlObject, check_type: str, column_name: str) -> object | None:
    #     if check_type == "unique":
    #         check_configs: dict = self._parse_check_configs(check)
    #         return self._create_check(f"duplicate_count({column_name}) = 0", check_configs)
    #     metric = f"duplicate_count({column_name})"
    #     return self._create_numeric_metric_check(metric, check)
    #
    # def _parse_column_aggregation_check(self, check: YamlObject, check_type: str, column_name: str) -> object | None:
    #     metric = f"{check_type}({column_name})"
    #     return self._create_numeric_metric_check(metric, check)
    #
    # def _parse_dataset_check(self, check: YamlObject, column_name: str) -> object | None:
    #     check_type = check.read_string("type")
    #
    #     if check_type == "row_count":
    #         return self._create_numeric_metric_check("row_count", check)
    #
    #     self.error(f"Unsupported dataset check type {check_type}: {check.location}")
