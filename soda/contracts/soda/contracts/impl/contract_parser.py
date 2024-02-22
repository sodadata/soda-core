from __future__ import annotations

import logging
from numbers import Number
from typing import Dict, List

from ruamel.yaml import CommentedMap

from soda.contracts.connection import SodaException
from soda.contracts.contract import (
    Check,
    Contract,
    FreshnessCheck,
    InvalidReferenceCheck,
    MissingConfigurations,
    NumericMetricCheck,
    NumericThreshold,
    Range,
    SchemaCheck,
    UserDefinedSqlExpressionCheck,
    UserDefinedSqlQueryCheck,
    ValidConfigurations,
    ValidReferenceColumn,
)
from soda.contracts.impl.json_schema_verifier import JsonSchemaVerifier
from soda.contracts.impl.logs import Logs
from soda.contracts.impl.variable_resolver import VariableResolver
from soda.contracts.impl.yaml import YamlList, YamlObject, YamlParser, YamlWrapper

logger = logging.getLogger(__name__)


class ContractParser:

    __threshold_keys = [
        "must_be_greater_than",
        "must_be_greater_than_or_equal_to",
        "must_be_less_than",
        "must_be_less_than_or_equal_to",
        "must_be",
        "must_not_be",
        "must_be_between",
        "must_be_not_between",
    ]

    __validity_keys = [
        "invalid_values",
        "invalid_format",
        "invalid_regex",
        "valid_values",
        "valid_format",
        "valid_regex",
        "valid_min",
        "valid_max",
        "valid_length",
        "valid_min_length",
        "valid_max_length",
        "valid_reference_column",
    ]

    def __init__(self):
        super().__init__()
        self.logs = Logs()
        self.missing_value_configs_by_column = {}
        self.skip_schema_validation: bool = False

    def parse_contract(self, contract_yaml_str: str, variables: dict[str, str] | None = None) -> Contract:
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

        dataset: str | None = contract_yaml_object.read_string("dataset")
        schema_name: str | None = contract_yaml_object.read_string_opt("schema")
        sql_filter: str | None = contract_yaml_object.read_string_opt("sql_filter")

        checks: list[Check] = []

        contract_columns_yaml_list: YamlList | None = contract_yaml_object.read_yaml_list("columns")
        if contract_columns_yaml_list:
            schema_columns: Dict[str, str | None] = {}
            schema_optional_columns: List[str] = []

            checks.append(
                SchemaCheck(
                    dataset=dataset,
                    column=None,
                    type="schema",
                    name="Schema",
                    contract_check_id=None,
                    location=None,
                    columns=schema_columns,
                    optional_columns=schema_optional_columns,
                )
            )

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
                                dataset=dataset,
                                contract_check_id=str(len(checks)),
                                check_yaml_object=column_check_yaml_object,
                                column=column_name,
                            )
                            if check:
                                checks.append(check)
                            else:
                                logging.error(f"Could not parse check for {column_check_yaml_object.unpacked()}")

        checks_yaml_list: YamlList | None = contract_yaml_object.read_yaml_list_opt("checks")
        if checks_yaml_list:
            for check_yaml_object in checks_yaml_list:
                check = self._parse_dataset_check(
                    contract_check_id=str(len(checks)), check_yaml_object=check_yaml_object
                )
                if check:
                    checks.append(check)

        return Contract(
            dataset=dataset,
            sql_filter=sql_filter,
            schema=schema_name,
            checks=checks,
            contract_yaml_str=contract_yaml_str,
            variables=variables,
            logs=self.logs,
        )

    def _parse_column_check(
        self,
        dataset: str,
        column: str,
        contract_check_id: str,
        check_yaml_object: YamlObject,
    ) -> Check | None:

        check_type = check_yaml_object.read_string("type")
        if check_type is None:
            return None

        if check_type in ["no_missing", "missing_count", "missing_percent"]:
            return self._parse_column_check_missing(
                dataset=dataset,
                column=column,
                contract_check_id=contract_check_id,
                check_yaml_object=check_yaml_object,
                check_type=check_type,
            )
        # elif check_type in ["no_invalid", "invalid_count", invalid_percent"]:
        # elif check_type in ["no_duplicates", "duplicate_count", "duplicate_percent"]:

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
            NumericThreshold(not_equal=0)
            if check_type
            in [
                "missing",
                "not_null",
                "missing_count",
                "missing_percent",
                "invalid",
                "invalid_count",
                "invalid_percent",
                "unique",
                "duplicate_count",
                "duplicate_percent",
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
                default_threshold=default_threshold,
            )
        elif check_type == "sql_expression":
            return self._parse_user_defined_sql_expression_check(
                contract_check_id=contract_check_id,
                check_yaml_object=check_yaml_object,
                check_type=check_type,
                column=column,
            )
        elif check_type.startswith("freshness_in_"):
            return self._parse_freshness_check(
                contract_check_id=contract_check_id,
                check_yaml_object=check_yaml_object,
                check_type=check_type,
                column=column,
            )

        return self._parse_numeric_metric_check(
            contract_check_id=contract_check_id,
            check_yaml_object=check_yaml_object,
            check_type=check_type,
            metric=metric,
            column=column,
            default_threshold=default_threshold,
        )

    def _parse_column_check_missing(
        self, dataset: str, contract_check_id: str, check_yaml_object: YamlObject, check_type: str, column: str | None
    ):
        name = check_yaml_object.read_string_opt("name")

        threshold: NumericThreshold = self._parse_numeric_threshold(check_yaml_object=check_yaml_object)

        metric: str = check_type

        if check_type == "no_missing":
            metric = "missing_count"
            if not threshold.is_empty():
                self.logs.error(
                    "Check type 'no_missing' does not allow for threshold keys must_be_...",
                    location=check_yaml_object.location,
                )
            threshold = NumericThreshold(equal=0)
        elif threshold.is_empty():
            self.logs.error(
                (
                    f"Check type '{check_type}' requires threshold configuration "
                    f"with keys like {self.__threshold_keys}"
                ),
                location=check_yaml_object.location,
            )

        missing_configurations: MissingConfigurations | None = self._parse_missing_configurations(
            check_yaml=check_yaml_object, column=column
        )

        return NumericMetricCheck(
            dataset=dataset,
            column=column,
            type=check_type,
            name=name,
            contract_check_id=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            metric=metric,
            missing_configurations=missing_configurations,
            valid_configurations=None,
            fail_threshold=threshold,
        )

    def _parse_column_check_invalid(
        self, contract_check_id: str, check_yaml_object: YamlObject, check_type: str, column: str | None
    ):
        name = check_yaml_object.read_string_opt("name")

        threshold: NumericThreshold = self._parse_numeric_threshold(check_yaml_object=check_yaml_object)

        metric: str = ""

        if check_type == "no_invalid":
            metric = "invalid_count"
            if threshold.is_empty():
                self.logs.error(
                    "Check type 'no_invalid' does not allow for threshold keys must_be_...",
                    location=check_yaml_object.location,
                )
        elif not threshold.is_empty():
            metric = check_type
            self.logs.error(
                (
                    f"Check type '{check_type}' requires threshold configuration "
                    f"with keys like {self.__threshold_keys}"
                ),
                location=check_yaml_object.location,
            )

        missing_configurations: MissingConfigurations | None = self._parse_missing_configurations(
            check_yaml=check_yaml_object, column=column
        )

        valid_configurations: ValidConfigurations | None = self._parse_valid_configurations(
            check_yaml=check_yaml_object
        )

        if not valid_configurations:
            self.logs.error(
                message=f"Check type '{check_type}' must have a validity configuration like {self.__validity_keys}",
                location=check_yaml_object.location,
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
            fail_threshold=threshold,
        )

    def _parse_numeric_metric_check(
        self,
        contract_check_id: str,
        check_yaml_object: YamlObject,
        check_type: str,
        metric: str,
        column: str | None,
        default_threshold: NumericThreshold | None,
    ) -> Check | None:

        name = check_yaml_object.read_string_opt("name")
        fail_threshold: NumericThreshold = self._parse_numeric_threshold_deprecated(
            check_yaml_object=check_yaml_object, prefix="fail_when_", default_threshold=default_threshold
        )

        missing_configurations: MissingConfigurations | None = self._parse_missing_configurations(
            check_yaml=check_yaml_object, column=column
        )

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
            warn_threshold=None,
        )

    def _parse_invalid_reference_check(
        self,
        contract_check_id: str,
        check_yaml_object: YamlObject,
        check_type: str,
        metric: str,
        column: str | None,
        valid_values_column_yaml_object: YamlObject,
        default_threshold: NumericThreshold | None,
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
            reference_column=reference_column,
        )

    def _parse_user_defined_sql_expression_check(
        self, contract_check_id: str, check_yaml_object: YamlObject, check_type: str, column: str | None
    ) -> Check | None:
        name = check_yaml_object.read_string_opt("name")
        metric: str = check_yaml_object.read_string("metric")
        sql_expression: str = check_yaml_object.read_string("metric_sql_expression")

        fail_threshold: NumericThreshold = self._parse_numeric_threshold_deprecated(
            check_yaml_object=check_yaml_object, prefix="fail_when_", default_threshold=None
        )

        if not fail_threshold:
            self.logs.error("No threshold defined for sql_expression check", location=check_yaml_object.location)

        for k in check_yaml_object:
            if k.startswith("warn_when"):
                self.logs.error(message=f"Warnings not yet supported: '{k}'", location=check_yaml_object.location)

        return UserDefinedSqlExpressionCheck(
            column=column,
            type=check_type,
            name=name,
            contract_check_id=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            metric=metric,
            sql_expression=sql_expression,
            fail_threshold=fail_threshold,
            warn_threshold=None,
        )

    def _parse_freshness_check(
        self, contract_check_id: str, check_yaml_object: YamlObject, check_type: str, column: str | None
    ) -> Check | None:
        name = check_yaml_object.read_string_opt("name")

        freshness_check_types = [
            "freshness_in_days",
            "freshness_in_hours",
            "freshness_in_minutes",
        ]
        if check_type not in freshness_check_types:
            self.logs.error(f"Invalid freshness check type: {check_type}: Expected one of {freshness_check_types}")
            return None

        fail_threshold: NumericThreshold = self._parse_numeric_threshold_deprecated(
            check_yaml_object=check_yaml_object, prefix="fail_when_", default_threshold=None
        )
        if not fail_threshold:
            self.logs.error("No threshold defined for sql_expression check", location=check_yaml_object.location)
        elif (
            fail_threshold.not_between is not None
            or fail_threshold.between is not None
            or fail_threshold.equal is not None
            or fail_threshold.not_equal is not None
            or fail_threshold.greater_than is not None
            or fail_threshold.less_than is not None
            or fail_threshold.less_than_or_equal is not None
        ):
            self.logs.error(
                "Invalid freshness threshold. Use fail_when_greater_than_or_equal", location=check_yaml_object.location
            )

        for k in check_yaml_object:
            if k.startswith("warn_when"):
                self.logs.error(message=f"Warnings not yet supported: '{k}'", location=check_yaml_object.location)

        return FreshnessCheck(
            column=column,
            type=check_type,
            name=name,
            contract_check_id=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            fail_threshold=fail_threshold,
            warn_threshold=None,
        )

    def _parse_missing_configurations(self, check_yaml: YamlObject, column: str) -> MissingConfigurations | None:
        missing_values_yaml_list: YamlList | None = check_yaml.read_yaml_list_opt(f"missing_values")
        missing_values: list | None = missing_values_yaml_list.unpacked() if missing_values_yaml_list else None
        missing_regex: str | None = check_yaml.read_string_opt(f"missing_regex")

        if all(v is None for v in [missing_values, missing_regex]):
            return self.missing_value_configs_by_column.get(column)

        else:
            missing_configurations = MissingConfigurations(missing_values=missing_values, missing_regex=missing_regex)

            # If a missing config is specified, do a complete overwrite.
            # Overwriting the missing configs gives more control to the contract author over merging the missing configs.
            self.missing_value_configs_by_column[column] = missing_configurations

            return missing_configurations

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
        valid_reference_column_yaml_object: YamlObject | None = check_yaml.read_yaml_object_opt(
            f"valid_reference_column"
        )
        if valid_reference_column_yaml_object:
            ref_dataset = valid_reference_column_yaml_object.read_string("dataset")
            ref_column = valid_reference_column_yaml_object.read_string("column")
            valid_reference_column = ValidReferenceColumn(dataset=ref_dataset, column=ref_column)

        if all(
            v is None
            for v in [
                invalid_values,
                invalid_format,
                invalid_regex,
                valid_values,
                valid_format,
                valid_regex,
                valid_min,
                valid_max,
                valid_length,
                valid_min_length,
                valid_max_length,
                valid_reference_column,
            ]
        ):
            return None
        else:
            return ValidConfigurations(
                invalid_values=invalid_values,
                invalid_format=invalid_format,
                invalid_regex=invalid_regex,
                valid_values=valid_values,
                valid_format=valid_format,
                valid_regex=valid_regex,
                valid_min=valid_min,
                valid_max=valid_max,
                valid_length=valid_length,
                valid_min_length=valid_min_length,
                valid_max_length=valid_max_length,
                valid_reference_column=valid_reference_column,
            )

    def _parse_numeric_threshold(self, check_yaml_object: YamlObject) -> NumericThreshold | None:
        numeric_threshold: NumericThreshold = NumericThreshold(
            greater_than=check_yaml_object.read_number_opt("must_be_greater_than"),
            greater_than_or_equal=check_yaml_object.read_number_opt("must_be_greater_than_or_equal_to"),
            less_than=check_yaml_object.read_number_opt("must_be_less_than"),
            less_than_or_equal=check_yaml_object.read_number_opt("must_be_less_than_or_equal_to"),
            equal=check_yaml_object.read_number_opt("must_be"),
            not_equal=check_yaml_object.read_number_opt("must_not_be"),
            between=self._parse_range(check_yaml_object, "must_be_between"),
            not_between=self._parse_range(check_yaml_object, "must_not_be_between"),
        )

        for key in check_yaml_object:
            if key.startswith("must_") and key not in self.__threshold_keys:
                self.logs.error(f"Invalid threshold '{key}'. Must be in '{self.__threshold_keys}'.")

        return numeric_threshold

    def _parse_numeric_threshold_deprecated(
        self, check_yaml_object: YamlObject, prefix: str, default_threshold: NumericThreshold | None
    ) -> NumericThreshold | None:

        numeric_threshold: NumericThreshold = NumericThreshold(
            greater_than=check_yaml_object.read_number_opt(f"{prefix}greater_than"),
            greater_than_or_equal=check_yaml_object.read_number_opt(f"{prefix}greater_than_or_equal"),
            less_than=check_yaml_object.read_number_opt(f"{prefix}less_than"),
            less_than_or_equal=check_yaml_object.read_number_opt(f"{prefix}less_than_or_equal"),
            equal=check_yaml_object.read_number_opt(f"{prefix}equal"),
            not_equal=check_yaml_object.read_number_opt(f"{prefix}not_equal"),
            between=self._parse_range(check_yaml_object, f"{prefix}between"),
            not_between=self._parse_range(check_yaml_object, f"{prefix}not_between"),
        )

        for key in check_yaml_object:
            if key.startswith(prefix) and key.endswith("equals"):
                self.logs.error(f"Invalid threshold '{key}'.  Did you mean '{key[:-1]}'?")

        return numeric_threshold if not numeric_threshold.is_empty() else default_threshold

    def _parse_range(self, check_yaml_object: YamlObject, range_key: str) -> Range | None:
        range_yaml_list: YamlList | None = check_yaml_object.read_yaml_list_opt(range_key)
        if isinstance(range_yaml_list, YamlList):
            range_values = range_yaml_list.unpacked()
            if all(isinstance(range_value, Number) for range_value in range_values) and len(range_values) == 2:
                return Range(lower_bound=range_values[0], upper_bound=range_values[1])
            else:
                self.logs.error("range expects a list of 2 numbers", location=range_yaml_list.location)

    def _parse_dataset_check(self, contract_check_id: str, check_yaml_object: YamlObject) -> Check | None:

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
                contract_check_id=contract_check_id, check_yaml_object=check_yaml_object, check_type=check_type
            )

        return self._parse_numeric_metric_check(
            contract_check_id=contract_check_id,
            check_yaml_object=check_yaml_object,
            check_type=check_type,
            metric=metric,
            column=None,
            default_threshold=NumericThreshold(not_equal=0),
        )

    def _parse_user_defined_sql_check(
        self,
        contract_check_id: str,
        check_yaml_object: YamlObject,
        check_type: str,
    ) -> Check | None:

        name = check_yaml_object.read_string_opt("name")
        metric: str = check_yaml_object.read_string("metric")
        query: str = check_yaml_object.read_string("query")

        fail_threshold: NumericThreshold = self._parse_numeric_threshold_deprecated(
            check_yaml_object=check_yaml_object, prefix="fail_when_", default_threshold=None
        )

        for k in check_yaml_object:
            if k.startswith("warn_when"):
                self.logs.error(message=f"Warnings not yet supported: '{k}'", location=check_yaml_object.location)

        return UserDefinedSqlQueryCheck(
            type=check_type,
            name=name,
            contract_check_id=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            metric=metric,
            query=query,
            fail_threshold=fail_threshold,
            warn_threshold=None,
        )
