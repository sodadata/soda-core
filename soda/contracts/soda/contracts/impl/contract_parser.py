from __future__ import annotations

import logging
from numbers import Number
from typing import List, Dict

from ruamel.yaml import CommentedMap

from soda.contracts.connection import SodaException
from soda.contracts.contract import Contract, Check, SchemaCheck, NumericThreshold, MissingConfigurations, \
    NumericMetricCheck, ValidReferenceColumn, ValidConfigurations, Range, InvalidReferenceCheck, UserDefinedSqlCheck
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
                type="schema",
                name="Schema",
                contract_check_id=None,
                location=None,
                columns=schema_columns,
                optional_columns=schema_optional_columns
            ))

            contract_column_yaml_object: YamlObject
            for contract_column_yaml_object in contract_columns_yaml_list.read_yaml_objects():
                column_name: str | None = contract_column_yaml_object.read_string("name")

                if isinstance(column_name, str):
                    data_type: str | None = contract_column_yaml_object.read_string_opt("data_type")
                    schema_columns[column_name] = data_type

                    if contract_column_yaml_object.read_bool_opt("optional", default_value=False):
                        schema_optional_columns.append(column_name)

                    column_check_yaml_objects: YamlList = contract_column_yaml_object.read_yaml_list_opt("checks")

                    if column_check_yaml_objects:
                        column_check_yaml_object: YamlObject
                        for column_check_yaml_object in column_check_yaml_objects:
                            check: Check = self._parse_column_check(
                                contract_check_id=str(len(checks)),
                                check_yaml_object=column_check_yaml_object,
                                column=column_name
                            )
                            if check:
                                checks.append(check)
                            else:
                                logging.error(f"Could not parse check for {column_check_yaml_object.unpacked()}")

        checks_yaml_list: YamlList | None = contract_yaml_object.read_yaml_list_opt("checks")
        if checks_yaml_list:
            for check_yaml_object in checks_yaml_list:
                check = self._parse_dataset_check(
                    contract_check_id=str(len(checks)),
                    check_yaml_object=check_yaml_object
                )
                if check:
                    checks.append(check)

        return Contract(
            dataset=dataset_name,
            schema=schema_name,
            checks=checks,
            contract_yaml_str=contract_yaml_str,
            logs=self.logs
        )

    def _parse_column_check(self,
                            contract_check_id: str,
                            check_yaml_object: YamlObject,
                            column: str
                            ) -> Check | None:

        check_type = check_yaml_object.read_string("type")
        if check_type is None:
            return None

        column_text = f"({column})" if column else ""

        metric: str
        if check_type in ["missing", "not_null"]:
            metric = f"missing_count{column_text}"
        elif check_type == "invalid":
            metric = f"invalid_count{column_text}"
        elif check_type == "unique":
            metric = f"duplicate_count{column_text}"
        else:
            metric = f"{check_type}{column_text}"

        default_threshold: NumericThreshold | None = (
            NumericThreshold(not_equals=0) if check_type in[
                "missing", "not_null", "missing_count", "missing_percent",
                "invalid", "invalid_count", "invalid_percent",
                "unique", "duplicate_count", "duplicate_percent"
            ]
            else None
        )

        valid_values_column_yaml_object: YamlObject = check_yaml_object.read_yaml_object_opt("valid_values_column")
        if metric.startswith("invalid_") and valid_values_column_yaml_object:
            return self._parse_invalid_reference_check(
            contract_check_id=contract_check_id,
            check_yaml_object=check_yaml_object,
            check_type=check_type,
            metric=metric,
            column=column,
            valid_values_column_yaml_object=valid_values_column_yaml_object,
            default_threshold=default_threshold
        )

        return self._parse_numeric_metric_check(
            contract_check_id=contract_check_id,
            check_yaml_object=check_yaml_object,
            check_type=check_type,
            metric=metric,
            column=column,
            default_threshold=default_threshold
        )

    def _parse_numeric_metric_check(self,
                                    contract_check_id: str,
                                    check_yaml_object: YamlObject,
                                    check_type: str,
                                    metric: str,
                                    column: str | None,
                                    default_threshold: NumericThreshold | None
                                    ) -> Check | None:

        name = check_yaml_object.read_string_opt("name")
        fail_threshold: NumericThreshold = self._parse_numeric_threshold(
            check_yaml_object=check_yaml_object,
            prefix="fail_when_",
            default_threshold=default_threshold
        )

        for k in check_yaml_object:
            if k.startswith("warn_when"):
                self.logs.error(message=f"Warnings not yet supported: '{k}'", location=check_yaml_object.location)

        missing_configurations: MissingConfigurations | None = self._parse_missing_configurations(
            check_yaml=check_yaml_object
        )
        if missing_configurations:
            # If a missing config is specified, do a complete overwrite.
            # Overwriting the missing configs gives more control to the contract author over merging the missing configs.
            self.missing_value_configs_by_column[column] = missing_configurations
        elif column is not None:
            missing_configurations = self.missing_value_configs_by_column.get(column)

        valid_configurations: ValidConfigurations | None = self._parse_valid_configurations(
            check_yaml=check_yaml_object
        )

        return NumericMetricCheck(
            type=check_type,
            name=name,
            contract_check_id=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            metric=metric,
            column=column,
            missing_configurations=missing_configurations,
            valid_configurations=valid_configurations,
            fail_threshold=fail_threshold,
            warn_threshold=None
        )

    def _parse_invalid_reference_check(self,
                                       contract_check_id: str,
                                       check_yaml_object: YamlObject,
                                       check_type: str,
                                       metric: str,
                                       column: str | None,
                                       valid_values_column_yaml_object: YamlObject,
                                       default_threshold: NumericThreshold | None
                                       ) -> Check | None:
        name = check_yaml_object.read_string_opt("name")

        reference_dataset = valid_values_column_yaml_object.read_string("dataset")
        reference_column = valid_values_column_yaml_object.read_string("column")

        return InvalidReferenceCheck(
            type=check_type,
            name=name,
            contract_check_id=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            column=column,
            reference_dataset=reference_dataset,
            reference_column=reference_column
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
                                 check_yaml_object: YamlObject,
                                 prefix: str,
                                 default_threshold: NumericThreshold | None
                                 ) -> NumericThreshold | None:
        greater_than = check_yaml_object.read_number_opt(f"{prefix}greater_than")
        greater_than_or_equal = check_yaml_object.read_number_opt(f"{prefix}greater_than_or_equal")
        less_than = check_yaml_object.read_number_opt(f"{prefix}less_than")
        less_than_or_equal = check_yaml_object.read_number_opt(f"{prefix}less_than_or_equal")
        equals = check_yaml_object.read_number_opt(f"{prefix}equals")
        not_equals = check_yaml_object.read_number_opt(f"{prefix}not_equals")
        between = self._parse_range(check_yaml_object, f"{prefix}between")
        not_between = self._parse_range(check_yaml_object, f"{prefix}not_between")

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

    def _parse_range(self, check_yaml_object: YamlObject, range_key: str) -> Range | None:
        range_yaml_list: YamlList | None = check_yaml_object.read_yaml_list_opt(range_key)
        if isinstance(range_yaml_list, YamlList):
            range_values = range_yaml_list.unpacked()
            if (
                all(isinstance(range_value, Number) for range_value in range_values)
                and len(range_values) == 2
            ):
                return Range(lower_bound=range_values[0], upper_bound=range_values[1])
            else:
                self.logs.error("range expects a list of 2 numbers", location=range_yaml_list.location)

    def _parse_dataset_check(self,
                             contract_check_id: str,
                             check_yaml_object: YamlObject
                             ) -> Check | None:

        check_type: str | None = check_yaml_object.read_string("type")
        if check_type is None:
            return None

        if check_type not in ["row_count", "multi_column_duplicates", "user_defined_sql"]:
            self.logs.error(f"Unknown dataset check type: {check_type}")
            return None

        metric: str = check_type

        if check_type == "multi_column_duplicates":
            columns: list[str] = check_yaml_object.read_list_strings("columns")
            columns_comma_separated = ", ".join(columns)
            metric = f"duplicate_count({columns_comma_separated})"

        elif check_type == "user_defined_sql":
            return self._parse_user_defined_sql_check(
                contract_check_id=contract_check_id,
                check_yaml_object=check_yaml_object,
                check_type=check_type
            )

        return self._parse_numeric_metric_check(
            contract_check_id=contract_check_id,
            check_yaml_object=check_yaml_object,
            check_type=check_type,
            metric=metric,
            column=None,
            default_threshold=NumericThreshold(not_equals=0)
        )

    def _parse_user_defined_sql_check(self,
                                      contract_check_id: str,
                                      check_yaml_object: YamlObject,
                                      check_type: str,
                                      ) -> Check | None:

        name = check_yaml_object.read_string_opt("name")
        metric: str = check_yaml_object.read_string("metric")
        query: str = check_yaml_object.read_string("query")

        fail_threshold: NumericThreshold = self._parse_numeric_threshold(
            check_yaml_object=check_yaml_object,
            prefix="fail_when_",
            default_threshold=None
        )

        for k in check_yaml_object:
            if k.startswith("warn_when"):
                self.logs.error(message=f"Warnings not yet supported: '{k}'", location=check_yaml_object.location)

        return UserDefinedSqlCheck(
            type=check_type,
            name=name,
            contract_check_id=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            metric=metric,
            query=query,
            fail_threshold=fail_threshold,
            warn_threshold=None
        )
