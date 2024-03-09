from __future__ import annotations

import logging
from numbers import Number
from typing import Dict, List

from ruamel.yaml import CommentedMap

from soda.contracts.connection import SodaException
from soda.contracts.contract import (
    Check,
    Contract,
    DuplicateCheck,
    FreshnessCheck,
    InvalidReferenceCheck,
    MissingConfigurations,
    NumericMetricCheck,
    NumericThreshold,
    Range,
    SchemaCheck,
    UserDefinedMetricSqlExpressionCheck,
    UserDefinedMetricSqlQueryCheck,
    ValidConfigurations,
    ValidValuesReferenceData,
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
        "invalid_sql_regex",
        "valid_values",
        "valid_format",
        "valid_sql_regex",
        "valid_min",
        "valid_max",
        "valid_length",
        "valid_min_length",
        "valid_max_length",
        "valid_values_reference_data",
    ]

    def __init__(self):
        super().__init__()
        self.logs: Logs = Logs()
        self.missing_value_configs_by_column: dict[str, MissingConfigurations] = {}
        self.valid_value_configs_by_column: dict[str, ValidConfigurations] = {}
        self.skip_schema_validation: bool = False

    def parse_contract(
        self, contract_yaml_str: str, variables: dict[str, str] | None, schedule: str | None
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

        dataset: str | None = contract_yaml_object.read_string("dataset")
        schema_name: str | None = contract_yaml_object.read_string_opt("schema")
        sql_filter: str | None = contract_yaml_object.read_string_opt("sql_filter")

        checks: dict[str, Check] = {}

        contract_columns_yaml_list: YamlList | None = contract_yaml_object.read_yaml_list("columns")
        if contract_columns_yaml_list:
            schema_columns: Dict[str, str | None] = {}
            schema_optional_columns: List[str] = []

            schema_check_identity: str = Check.create_check_identity(
                schedule=schedule,
                dataset=dataset,
                column=None,
                check_type="schema",
                check_identity_suffix=None,
                check_location=None,
                checks=checks,
                logs=self.logs,
            )

            checks[schema_check_identity] = SchemaCheck(
                schedule=schedule,
                dataset=dataset,
                column=None,
                type="schema",
                name="Schema",
                identity=schema_check_identity,
                location=None,
                columns=schema_columns,
                optional_columns=schema_optional_columns,
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
                            check_type: str | None = column_check_yaml_object.read_string("type")
                            check_identity_suffix: str | None = column_check_yaml_object.read_string_opt(
                                "identity_suffix"
                            )
                            if check_type is not None:
                                check_identity: str = Check.create_check_identity(
                                    schedule=schedule,
                                    dataset=dataset,
                                    column=column_name,
                                    check_type=check_type,
                                    check_identity_suffix=check_identity_suffix,
                                    check_location=column_check_yaml_object.location,
                                    checks=checks,
                                    logs=self.logs,
                                )
                                check: Check = self._parse_column_check(
                                    schedule=schedule,
                                    dataset=dataset,
                                    contract_check_id=check_identity,
                                    check_yaml_object=column_check_yaml_object,
                                    column=column_name,
                                    check_type=check_type,
                                )
                                if check:
                                    checks[check_identity] = check
                                else:
                                    logging.error(f"Could not parse check for {column_check_yaml_object.unpacked()}")

        checks_yaml_list: YamlList | None = contract_yaml_object.read_yaml_list_opt("checks")
        if checks_yaml_list:
            for check_yaml_object in checks_yaml_list:
                check_type: str | None = check_yaml_object.read_string("type")
                check_identity_suffix: str | None = check_yaml_object.read_string_opt("identity_suffix")
                if check_type is not None:
                    check_identity: str = Check.create_check_identity(
                        schedule=schedule,
                        dataset=dataset,
                        column=None,
                        check_type=check_type,
                        check_identity_suffix=check_identity_suffix,
                        check_location=check_yaml_object.location,
                        checks=checks,
                        logs=self.logs,
                    )
                    check = self._parse_dataset_check(
                        schedule=schedule,
                        dataset=dataset,
                        contract_check_id=check_identity,
                        check_yaml_object=check_yaml_object,
                        check_type=check_type,
                    )
                    if check:
                        checks[check_identity] = check

        return Contract(
            schedule=schedule,
            dataset=dataset,
            sql_filter=sql_filter,
            schema=schema_name,
            checks=list(checks.values()),
            contract_yaml_str=contract_yaml_str,
            variables=variables,
            logs=self.logs,
        )

    def _parse_column_check(
        self,
        schedule: str | None,
        dataset: str,
        column: str,
        contract_check_id: str,
        check_yaml_object: YamlObject,
        check_type: str,
    ) -> Check | None:

        if check_type in ["no_missing_values", "missing_count", "missing_percent"]:
            parse_check_function = self._parse_column_check_missing
        elif check_type in ["no_invalid_values", "invalid_count", "invalid_percent"]:
            parse_check_function = self._parse_column_check_invalid
        elif check_type in ["no_duplicate_values", "duplicate_count", "duplicate_percent"]:
            parse_check_function = self._parse_check_duplicate
        elif check_type == "metric_sql_expression":
            parse_check_function = self._parse_user_defined_metric_sql_expression_check
        elif check_type.startswith("freshness_in_"):
            parse_check_function = self._parse_freshness_check
        elif check_type in ["avg", "sum"]:
            parse_check_function = self._parse_column_check_basic_sql_function
        else:
            self.logs.error(
                f"Invalid column check type '{check_type}' (column '{column}')", location=check_yaml_object.location
            )
            return None

        return parse_check_function(
            schedule=schedule,
            dataset=dataset,
            column=column,
            contract_check_id=contract_check_id,
            check_yaml_object=check_yaml_object,
            check_type=check_type,
        )

    def _parse_column_check_missing(
        self,
        schedule: str | None,
        dataset: str,
        contract_check_id: str,
        check_yaml_object: YamlObject,
        check_type: str,
        column: str | None,
    ):
        name = check_yaml_object.read_string_opt("name")

        threshold: NumericThreshold = self._parse_numeric_threshold(check_yaml_object=check_yaml_object)

        metric: str = check_type

        if check_type == "no_missing_values":
            metric = "missing_count"
            if not threshold.is_empty():
                self.logs.error(
                    f"Check type 'no_missing_values' does not allow for threshold keys must_... (column '{column}')",
                    location=check_yaml_object.location,
                )
            threshold = NumericThreshold(equal=0)
        elif threshold.is_empty():
            self.logs.error(
                (
                    f"Check type '{check_type}' requires threshold configuration "
                    f"with keys like {self.__threshold_keys} (column '{column}')"
                ),
                location=check_yaml_object.location,
            )

        missing_configurations: MissingConfigurations | None = self._parse_missing_configurations(
            check_yaml=check_yaml_object, column=column
        )

        return NumericMetricCheck(
            schedule=schedule,
            dataset=dataset,
            column=column,
            type=check_type,
            name=name,
            identity=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            metric=metric,
            missing_configurations=missing_configurations,
            valid_configurations=None,
            threshold=threshold,
        )

    def _parse_column_check_invalid(
        self,
        schedule: str | None,
        dataset: str,
        contract_check_id: str,
        check_yaml_object: YamlObject,
        check_type: str,
        column: str | None,
    ):
        name = check_yaml_object.read_string_opt("name")

        threshold: NumericThreshold = self._parse_numeric_threshold(check_yaml_object=check_yaml_object)

        metric: str = check_type

        if check_type == "no_invalid_values":
            metric = "invalid_count"
            if not threshold.is_empty():
                self.logs.error(
                    f"Check type 'no_invalid_values' does not allow for threshold "
                    f"keys must_... (column '{column}')",
                    location=check_yaml_object.location,
                )
            threshold = NumericThreshold(equal=0)
        elif threshold.is_empty():
            self.logs.error(
                (
                    f"Check type '{check_type}' requires threshold configuration "
                    f"with keys like {self.__threshold_keys} (column '{column}')"
                ),
                location=check_yaml_object.location,
            )

        missing_configurations: MissingConfigurations | None = self._parse_missing_configurations(
            check_yaml=check_yaml_object, column=column
        )

        valid_configurations: ValidConfigurations | None = self._parse_valid_configurations(
            check_yaml=check_yaml_object, column=column
        )

        if not valid_configurations:
            self.logs.error(
                message=f"Check type '{check_type}' must have a validity configuration like {self.__validity_keys} "
                f"(column '{column}')",
                location=check_yaml_object.location,
            )
            return None

        if valid_configurations.valid_values_reference_data is not None:
            if valid_configurations.has_non_reference_data_configs():
                self.logs.error(
                    message=f"Check type '{check_type}' cannot combine 'valid_values_reference_data' with other "
                    f"validity configurations (column '{column}')",
                    location=check_yaml_object.location,
                )
                return None

            return InvalidReferenceCheck(
                schedule=schedule,
                dataset=dataset,
                column=column,
                type=check_type,
                name=name,
                identity=contract_check_id,
                location=check_yaml_object.location,
                metric="invalid_count",
                check_yaml_object=check_yaml_object,
                missing_configurations=missing_configurations,
                valid_configurations=valid_configurations,
                threshold=threshold,
                valid_values_reference_data=valid_configurations.valid_values_reference_data,
            )

        return NumericMetricCheck(
            schedule=schedule,
            dataset=dataset,
            column=column,
            type=check_type,
            name=name,
            identity=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            metric=metric,
            missing_configurations=missing_configurations,
            valid_configurations=valid_configurations,
            threshold=threshold,
        )

    def _parse_check_duplicate(
        self,
        schedule: str | None,
        dataset: str,
        contract_check_id: str,
        check_yaml_object: YamlObject,
        check_type: str,
        column: str | None,
    ):
        """
        Parses both the column level duplicate checks as well as the dataset level multi column duplicate checks.
        """

        name = check_yaml_object.read_string_opt("name")

        threshold: NumericThreshold = self._parse_numeric_threshold(check_yaml_object=check_yaml_object)

        metric: str = check_type

        if check_type == "no_duplicate_values":
            metric = "duplicate_count"
            if not threshold.is_empty():
                self.logs.error(
                    f"Check type 'no_duplicate_values' does not allow for threshold keys must_... (column '{column}')",
                    location=check_yaml_object.location,
                )
            threshold = NumericThreshold(equal=0)
        elif threshold.is_empty():
            self.logs.error(
                (
                    f"Check type '{check_type}' requires threshold configuration "
                    f"with keys like {self.__threshold_keys} (column '{column}')"
                ),
                location=check_yaml_object.location,
            )

        missing_configurations: MissingConfigurations | None = None
        valid_configurations: ValidConfigurations | None = None
        columns: list[str] | None = None

        if column is None:
            # Means this is a dataset (multi-column) duplicate check
            columns = check_yaml_object.read_list_strings("columns")

        else:
            # Means this is a (single) column duplicate check
            missing_configurations = self._parse_missing_configurations(check_yaml=check_yaml_object, column=column)
            valid_configurations = self._parse_valid_configurations(check_yaml=check_yaml_object, column=column)

        return DuplicateCheck(
            schedule=schedule,
            dataset=dataset,
            column=column,
            type=check_type,
            name=name,
            identity=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            metric=metric,
            missing_configurations=missing_configurations,
            valid_configurations=valid_configurations,
            threshold=threshold,
            columns=columns,
        )

    def _parse_column_check_basic_sql_function(
        self,
        schedule: str | None,
        dataset: str,
        contract_check_id: str,
        check_yaml_object: YamlObject,
        check_type: str,
        column: str | None,
    ):
        name = check_yaml_object.read_string_opt("name")

        threshold: NumericThreshold = self._parse_numeric_threshold(check_yaml_object=check_yaml_object)

        metric: str = check_type

        return NumericMetricCheck(
            schedule=schedule,
            dataset=dataset,
            column=column,
            type=check_type,
            name=name,
            identity=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            metric=metric,
            missing_configurations=None,
            valid_configurations=None,
            threshold=threshold,
        )

    def _parse_user_defined_metric_sql_expression_check(
        self,
        schedule: str | None,
        dataset: str,
        contract_check_id: str,
        check_yaml_object: YamlObject,
        check_type: str,
        column: str | None,
    ) -> Check | None:
        name = check_yaml_object.read_string_opt("name")
        metric: str = check_yaml_object.read_string("metric")
        sql_expression: str = check_yaml_object.read_string("sql_expression")

        threshold: NumericThreshold = self._parse_numeric_threshold(check_yaml_object=check_yaml_object)

        if not threshold:
            self.logs.error("No threshold defined for metric_sql_expression check", location=check_yaml_object.location)

        return UserDefinedMetricSqlExpressionCheck(
            schedule=schedule,
            dataset=dataset,
            column=column,
            type=check_type,
            name=name,
            identity=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            metric=metric,
            missing_configurations=None,
            valid_configurations=None,
            threshold=threshold,
            metric_sql_expression=sql_expression,
        )

    def _parse_freshness_check(
        self,
        schedule: str | None,
        dataset: str,
        contract_check_id: str,
        check_yaml_object: YamlObject,
        check_type: str,
        column: str | None,
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

        threshold: NumericThreshold = self._parse_numeric_threshold(check_yaml_object=check_yaml_object)
        if not threshold:
            self.logs.error("No threshold defined for freshness check", location=check_yaml_object.location)
        elif (
            threshold.not_between is not None
            or threshold.between is not None
            or threshold.equal is not None
            or threshold.not_equal is not None
            or threshold.greater_than is not None
            or threshold.greater_than is not None
            or threshold.less_than_or_equal is not None
        ):
            self.logs.error("Invalid freshness threshold. Use must_be_less_than", location=check_yaml_object.location)

        return FreshnessCheck(
            schedule=schedule,
            dataset=dataset,
            column=column,
            type=check_type,
            name=name,
            identity=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            threshold=threshold,
        )

    def _parse_missing_configurations(self, check_yaml: YamlObject, column: str) -> MissingConfigurations | None:
        missing_values_yaml_list: YamlList | None = check_yaml.read_yaml_list_opt(f"missing_values")
        missing_values: list | None = missing_values_yaml_list.unpacked() if missing_values_yaml_list else None
        missing_sql_regex: str | None = check_yaml.read_string_opt(f"missing_sql_regex")

        if all(v is None for v in [missing_values, missing_sql_regex]):
            return self.missing_value_configs_by_column.get(column)

        else:
            missing_configurations = MissingConfigurations(
                missing_values=missing_values, missing_sql_regex=missing_sql_regex
            )

            # If a missing config is specified, do a complete overwrite.
            # Overwriting the missing configs gives more control to the contract author over merging the missing configs.
            self.missing_value_configs_by_column[column] = missing_configurations

            return missing_configurations

    def _parse_valid_configurations(self, check_yaml: YamlObject, column: str) -> ValidConfigurations | None:
        invalid_values_yaml_list: YamlList | None = check_yaml.read_yaml_list_opt(f"invalid_values")
        invalid_values: list | None = invalid_values_yaml_list.unpacked() if invalid_values_yaml_list else None
        invalid_format: str | None = check_yaml.read_string_opt(f"invalid_format")
        invalid_sql_regex: str | None = check_yaml.read_string_opt(f"invalid_sql_regex")

        valid_values_yaml_list: YamlList | None = check_yaml.read_yaml_list_opt(f"valid_values")
        valid_values: list | None = valid_values_yaml_list.unpacked() if valid_values_yaml_list else None

        valid_format: str | None = check_yaml.read_string_opt(f"valid_format")
        valid_sql_regex: str | None = check_yaml.read_string_opt(f"valid_sql_regex")

        valid_min: Number | None = check_yaml.read_number_opt(f"valid_min")
        valid_max: Number | None = check_yaml.read_number_opt(f"valid_max")

        valid_length: int | None = check_yaml.read_number_opt(f"valid_length")
        valid_min_length: int | None = check_yaml.read_number_opt(f"valid_min_length")
        valid_max_length: int | None = check_yaml.read_number_opt(f"valid_max_length")

        valid_values_reference_data: ValidValuesReferenceData | None = None
        valid_values_reference_data_yaml_object: YamlObject | None = check_yaml.read_yaml_object_opt(
            f"valid_values_reference_data"
        )
        if valid_values_reference_data_yaml_object:
            ref_dataset = valid_values_reference_data_yaml_object.read_string("dataset")
            ref_column = valid_values_reference_data_yaml_object.read_string("column")
            valid_values_reference_data = ValidValuesReferenceData(dataset=ref_dataset, column=ref_column)

        if all(
            v is None
            for v in [
                invalid_values,
                invalid_format,
                invalid_sql_regex,
                valid_values,
                valid_format,
                valid_sql_regex,
                valid_min,
                valid_max,
                valid_length,
                valid_min_length,
                valid_max_length,
                valid_values_reference_data,
            ]
        ):
            return self.valid_value_configs_by_column.get(column)
        else:
            valid_configurations = ValidConfigurations(
                invalid_values=invalid_values,
                invalid_format=invalid_format,
                invalid_sql_regex=invalid_sql_regex,
                valid_values=valid_values,
                valid_format=valid_format,
                valid_sql_regex=valid_sql_regex,
                valid_min=valid_min,
                valid_max=valid_max,
                valid_length=valid_length,
                valid_min_length=valid_min_length,
                valid_max_length=valid_max_length,
                valid_values_reference_data=valid_values_reference_data,
            )

            # If a valid config is specified, do a complete overwrite.
            # Overwriting the valid configs gives more control to the contract author over merging the missing configs.
            self.valid_value_configs_by_column[column] = valid_configurations

            return valid_configurations

    def _parse_numeric_threshold(self, check_yaml_object: YamlObject) -> NumericThreshold | None:
        numeric_threshold: NumericThreshold = NumericThreshold(
            greater_than=check_yaml_object.read_number_opt("must_be_greater_than"),
            greater_than_or_equal=check_yaml_object.read_number_opt("must_be_greater_than_or_equal_to"),
            less_than=check_yaml_object.read_number_opt("must_be_less_than"),
            less_than_or_equal=check_yaml_object.read_number_opt("must_be_less_than_or_equal_to"),
            equal=check_yaml_object.read_number_opt("must_be"),
            not_equal=check_yaml_object.read_number_opt("must_not_be"),
            between=self._parse_range(check_yaml_object, "must_be_between"),
            not_between=self._parse_range(check_yaml_object, "must_be_not_between"),
        )

        for key in check_yaml_object:
            if key.startswith("must_") and key not in self.__threshold_keys:
                self.logs.error(f"Invalid threshold '{key}'. Must be in '{self.__threshold_keys}'.")

        return numeric_threshold

    def _parse_range(self, check_yaml_object: YamlObject, range_key: str) -> Range | None:
        range_yaml_list: YamlList | None = check_yaml_object.read_yaml_list_opt(range_key)
        if isinstance(range_yaml_list, YamlList):
            range_values = range_yaml_list.unpacked()
            if all(isinstance(range_value, Number) for range_value in range_values) and len(range_values) == 2:
                return Range(lower_bound=range_values[0], upper_bound=range_values[1])
            else:
                self.logs.error("range expects a list of 2 numbers", location=range_yaml_list.location)

    def _parse_dataset_check(
        self, schedule: str | None, dataset: str, contract_check_id: str, check_yaml_object: YamlObject, check_type: str
    ) -> Check | None:

        if check_type in ["rows_exist", "row_count"]:
            check_parse_function = self._parse_dataset_row_count
        elif check_type in ["no_duplicate_values", "duplicate_count", "duplicate_percent"]:
            check_parse_function = self._parse_check_duplicate
        elif check_type == "metric_sql_expression":
            check_parse_function = self._parse_user_defined_metric_sql_expression_check
        elif check_type == "metric_sql_query":
            check_parse_function = self._parse_user_defined_metric_sql_query_check
        elif check_type == "failed_rows_sql_query":
            check_parse_function = self._parse_user_defined_failed_rows_query_check
        else:
            self.logs.error(f"Invalid dataset check type '{check_type}'", location=check_yaml_object.location)
            return None

        return check_parse_function(
            schedule=schedule,
            dataset=dataset,
            column=None,
            contract_check_id=contract_check_id,
            check_yaml_object=check_yaml_object,
            check_type=check_type,
        )

    def _parse_dataset_row_count(
        self,
        schedule: str | None,
        dataset: str,
        contract_check_id: str,
        check_yaml_object: YamlObject,
        check_type: str,
        column: str | None,
    ) -> NumericMetricCheck:
        name = check_yaml_object.read_string_opt("name")

        threshold: NumericThreshold = self._parse_numeric_threshold(check_yaml_object=check_yaml_object)

        metric: str = check_type

        if check_type == "rows_exist":
            metric = "row_count"
            if not threshold.is_empty():
                self.logs.error(
                    f"Check type 'rows_exist' does not allow for threshold keys must_... (column '{column}')",
                    location=check_yaml_object.location,
                )
            threshold = NumericThreshold(greater_than=0)
        elif threshold.is_empty():
            self.logs.error(
                (
                    f"Check type '{check_type}' requires threshold configuration "
                    f"with keys like {self.__threshold_keys} (column '{column}')"
                ),
                location=check_yaml_object.location,
            )

        return NumericMetricCheck(
            schedule=schedule,
            dataset=dataset,
            column=column,
            type=check_type,
            name=name,
            identity=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            metric=metric,
            missing_configurations=None,
            valid_configurations=None,
            threshold=threshold,
        )

    def _parse_user_defined_metric_sql_query_check(
        self,
        schedule: str | None,
        dataset: str,
        column: None,
        contract_check_id: str,
        check_yaml_object: YamlObject,
        check_type: str,
    ) -> Check | None:

        name = check_yaml_object.read_string_opt("name")
        metric: str = check_yaml_object.read_string("metric")
        metric_sql_query: str = check_yaml_object.read_string("sql_query")

        threshold: NumericThreshold = self._parse_numeric_threshold(check_yaml_object=check_yaml_object)

        return UserDefinedMetricSqlQueryCheck(
            schedule=schedule,
            dataset=dataset,
            column=None,
            type=check_type,
            name=name,
            identity=contract_check_id,
            location=check_yaml_object.location,
            check_yaml_object=check_yaml_object,
            metric=metric,
            missing_configurations=None,
            valid_configurations=None,
            threshold=threshold,
            metric_sql_query=metric_sql_query,
        )
