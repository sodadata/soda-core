from __future__ import annotations

import logging
from dataclasses import dataclass
from numbers import Number
from textwrap import indent
from typing import List

from soda.common import logs as soda_core_logs
from soda.contracts.check import Check, MissingConfigurations, ValidConfigurations, SchemaCheck, \
    Threshold, MissingCheckFactory, InvalidCheckFactory, DuplicateCheckFactory, ValidValuesReferenceData, \
    AbstractCheck, UserDefinedMetricExpressionCheckFactory, SqlFunctionCheckFactory, CheckFactory, CheckResult, \
    CheckOutcome, FreshnessCheckFactory, CheckArgs, UserDefinedMetricQueryCheckFactory, \
    MultiColumnDuplicateCheckFactory, RowCountCheckFactory
from soda.contracts.data_source import DataSource, FileClDataSource
from soda.contracts.impl.json_schema_verifier import JsonSchemaVerifier
from soda.contracts.impl.logs import Location, Log, LogLevel, Logs
from soda.contracts.impl.yaml_helper import YamlHelper, YamlFile
from soda.scan import Scan
from soda.scan import logger as scan_logger

logger = logging.getLogger(__name__)


class Contract:

    @classmethod
    def create(cls, data_source: DataSource, contract_file: YamlFile, variables: dict[str, str], logs: Logs):
        return Contract(data_source=data_source, contract_file=contract_file, variables=variables, logs=logs)

    def __init__(self, data_source: DataSource, contract_file: YamlFile, variables: dict[str, str], logs: Logs):
        self.data_source: DataSource = data_source
        self.contract_file: YamlFile = contract_file
        self.variables: dict[str, str] = variables
        self.logs: Logs = logs

        self.dataset: str | None = None
        self.schema: str | None = None
        # TODO explain filter_expression_sql, default filter and named filters
        # filter name must part of the identity of the metrics
        #   - no filter part if no filter is specified
        #   - "default" is the filter name if there is only a default specified with "filter_expression_sql"
        #   - {filter_name} if a filter is activated from a named map of filters
        self.filter: str | None = None

        # TODO explain verification context
        self.verification_context: str | None = None

        self.filter_sql: str | None = None
        self.checks: list[Check] = []

        self.missing_value_configs_by_column: dict[str, MissingConfigurations] = {}
        self.valid_value_configs_by_column: dict[str, ValidConfigurations] = {}

        self.parse()

    def parse(self) -> Contract:
        """
        Dry run: parse but not verify the contract to get the errors in the logs.
        """
        already_parsed: bool = isinstance(self.verification_context, str)
        if not already_parsed:
            try:
                yaml_helper = YamlHelper(yaml_file=self.contract_file, logs=self.logs)

                self.contract_file.parse(self.variables)
                if not self.contract_file.is_ok():
                    return self

                # Verify the contract schema on the ruamel instance object
                json_schema_verifier: JsonSchemaVerifier = JsonSchemaVerifier(self.logs)
                json_schema_verifier.verify(self.contract_file.dict)

                contract_yaml_dict = self.contract_file.dict

                self.data_source_name: str | None = yaml_helper.read_string_opt(contract_yaml_dict, "data_source")
                self.schema: str | None = yaml_helper.read_string_opt(contract_yaml_dict, "schema")
                self.dataset: str | None = yaml_helper.read_string(contract_yaml_dict, "dataset")
                self.filter_sql: str | None = yaml_helper.read_string_opt(
                    contract_yaml_dict,
                    "filter_sql"
                )
                self.filter: str | None = "default" if self.filter_sql else None

                # Computing the verification_context
                verification_context_dict: dict = self.variables.copy()
                verification_context_dict.update({
                    "data_source": self.data_source,
                    "schema": self.schema,
                    "dataset": self.dataset,
                    "filter": self.filter,
                })
                verification_context_parts: list = [
                    f"{k}={v}" for k, v in verification_context_dict.items() if v
                ]
                self.verification_context = ",".join(verification_context_parts)

                self.checks.append(SchemaCheck(
                    logs=self.logs,
                    contract_file=self.contract_file,
                    verification_context=self.verification_context,
                    yaml_contract=contract_yaml_dict
                ))

                column_yamls: list | None = yaml_helper.read_list(contract_yaml_dict, "columns")
                if column_yamls:
                    for column_yaml in column_yamls:
                        column: str | None = yaml_helper.read_string(column_yaml, "name")
                        check_yamls: list | None = yaml_helper.read_list_opt(column_yaml, "checks")
                        if column and check_yamls:
                            for check_yaml in check_yamls:
                                check_type: str | None = yaml_helper.read_string(check_yaml, "type")
                                check_name = yaml_helper.read_string_opt(check_yaml,"name")

                                missing_configurations: MissingConfigurations | None = self.__parse_missing_configurations(
                                    check_yaml=check_yaml, column=column
                                )
                                valid_configurations: ValidConfigurations | None = self.__parse_valid_configurations(
                                    check_yaml=check_yaml, column=column
                                )
                                threshold: Threshold = self.__parse_numeric_threshold(
                                    check_yaml=check_yaml
                                )

                                location: Location = yaml_helper.create_location_from_yaml_value(check_yaml, self.contract_file.file_path)

                                check_args: CheckArgs = CheckArgs(
                                    logs=self.logs,
                                    contract_file=self.contract_file,
                                    verification_context=self.verification_context,
                                    check_type=check_type,
                                    check_yaml=check_yaml,
                                    check_name=check_name,
                                    threshold=threshold,
                                    location=location,
                                    yaml_helper=yaml_helper,
                                    column=column,
                                    missing_configurations=missing_configurations,
                                    valid_configurations=valid_configurations,
                                )

                                column_check_factory_classes: list[CheckFactory] = [
                                    MissingCheckFactory(),
                                    InvalidCheckFactory(),
                                    DuplicateCheckFactory(),
                                    UserDefinedMetricExpressionCheckFactory(),
                                    UserDefinedMetricQueryCheckFactory(),
                                    FreshnessCheckFactory(),
                                    SqlFunctionCheckFactory(),
                                ]

                                check: Check = self.__create_check(check_args, column_check_factory_classes)
                                if check:
                                    self.checks.append(check)
                                else:
                                    self.logs.error(
                                        message=f"Invalid column {check_args.check_type} check",
                                        location=check_args.location
                                    )

                check_yamls: list | None = yaml_helper.read_list_opt(contract_yaml_dict, "checks")
                if check_yamls:
                    for check_yaml in check_yamls:
                        check_type: str | None = yaml_helper.read_string(check_yaml, "type")
                        check_name = yaml_helper.read_string_opt(check_yaml,"name")
                        threshold: Threshold = self.__parse_numeric_threshold(
                            check_yaml=check_yaml
                        )

                        location: Location = yaml_helper.create_location_from_yaml_value(check_yaml, self.contract_file.file_path)

                        check_args: CheckArgs = CheckArgs(
                            logs=self.logs,
                            contract_file=self.contract_file,
                            verification_context=self.verification_context,
                            check_type=check_type,
                            check_yaml=check_yaml,
                            check_name=check_name,
                            threshold=threshold,
                            location=location,
                            yaml_helper=yaml_helper,
                        )

                        dataset_check_factory_classes: list[CheckFactory] = [
                            UserDefinedMetricExpressionCheckFactory(),
                            UserDefinedMetricQueryCheckFactory(),
                            RowCountCheckFactory(),
                            MultiColumnDuplicateCheckFactory(),
                        ]

                        check: Check = self.__create_check(check_args, dataset_check_factory_classes)
                        if check:
                            self.checks.append(check)
                        else:
                            self.logs.error(
                                message=f"Invalid dataset {check_args.check_type} check",
                                location=check_args.location
                            )

                checks_by_identity: dict[str, Check] = {}
                for check in self.checks:
                    if check.identity in checks_by_identity:
                        other_check: Check = checks_by_identity[check.identity]
                        if other_check:
                            location_info: str = ""
                            if isinstance(check, AbstractCheck) and isinstance(other_check, AbstractCheck):
                                location_info = f": {other_check.location} and {check.location}"
                            self.logs.error(f"Duplicate check identity '{check.identity}'{location_info}")
                    else:
                        checks_by_identity[check.identity] = check

            except Exception as e:
                self.logs.error(
                    message=f"Could not verify contract: {e}",
                    exception=e
                )
        return self

    def __parse_missing_configurations(self, check_yaml: dict, column: str) -> MissingConfigurations | None:
        yaml_helper: YamlHelper = YamlHelper(self.logs)
        missing_values: list | None = yaml_helper.read_list_opt(check_yaml, "missing_values")
        missing_regex_sql: str | None = yaml_helper.read_string_opt(check_yaml, "missing_regex_sql")

        if all(v is None for v in [missing_values, missing_regex_sql]):
            return self.missing_value_configs_by_column.get(column)

        else:
            missing_configurations = MissingConfigurations(
                missing_values=missing_values, missing_regex_sql=missing_regex_sql
            )

            # If a missing config is specified, do a complete overwrite.
            # Overwriting the missing configs gives more control to the contract author over merging the missing configs.
            self.missing_value_configs_by_column[column] = missing_configurations

            return missing_configurations

    def __parse_valid_configurations(self, check_yaml: dict, column: str) -> ValidConfigurations | None:
        yaml_helper: YamlHelper = YamlHelper(self.logs)

        invalid_values: list | None = yaml_helper.read_list_opt(check_yaml, "invalid_values")
        invalid_format: str | None = yaml_helper.read_string_opt(check_yaml, "invalid_format")
        invalid_regex_sql: str | None = yaml_helper.read_string_opt(check_yaml, "invalid_regex_sql")

        valid_values: list | None = yaml_helper.read_list_opt(check_yaml, "valid_values")

        valid_format: str | None = yaml_helper.read_string_opt(check_yaml, "valid_format")
        valid_regex_sql: str | None = yaml_helper.read_string_opt(check_yaml, "valid_regex_sql")

        valid_min: Number | None = yaml_helper.read_number_opt(check_yaml, "valid_min")
        valid_max: Number | None = yaml_helper.read_number_opt(check_yaml, "valid_max")

        valid_length: int | None = yaml_helper.read_number_opt(check_yaml, "valid_length")
        valid_min_length: int | None = yaml_helper.read_number_opt(check_yaml, "valid_min_length")
        valid_max_length: int | None = yaml_helper.read_number_opt(check_yaml, "valid_max_length")

        valid_values_reference_data: ValidValuesReferenceData | None = None
        valid_values_reference_data_yaml_object: dict | None = yaml_helper.read_dict_opt(
            check_yaml,
            f"valid_values_reference_data"
        )
        if valid_values_reference_data_yaml_object:
            ref_dataset = yaml_helper.read_string(valid_values_reference_data_yaml_object, "dataset")
            ref_column = yaml_helper.read_string(valid_values_reference_data_yaml_object, "column")
            valid_values_reference_data = ValidValuesReferenceData(dataset=ref_dataset, column=ref_column)

        if all(
            v is None
            for v in [
                invalid_values,
                invalid_format,
                invalid_regex_sql,
                valid_values,
                valid_format,
                valid_regex_sql,
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
                invalid_regex_sql=invalid_regex_sql,
                valid_values=valid_values,
                valid_format=valid_format,
                valid_regex_sql=valid_regex_sql,
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

    def __parse_numeric_threshold(self, check_yaml: dict) -> Threshold | None:
        yaml_helper: YamlHelper = YamlHelper(self.logs)

        numeric_threshold: Threshold = Threshold(
            greater_than=yaml_helper.read_number_opt(check_yaml, "must_be_greater_than"),
            greater_than_or_equal=yaml_helper.read_number_opt(check_yaml, "must_be_greater_than_or_equal_to"),
            less_than=yaml_helper.read_number_opt(check_yaml, "must_be_less_than"),
            less_than_or_equal=yaml_helper.read_number_opt(check_yaml, "must_be_less_than_or_equal_to"),
            equal=yaml_helper.read_number_opt(check_yaml, "must_be"),
            not_equal=yaml_helper.read_number_opt(check_yaml, "must_not_be"),
            between=yaml_helper.read_range(check_yaml, "must_be_between"),
            not_between=yaml_helper.read_range(check_yaml, "must_be_not_between"),
        )

        for key in check_yaml:
            if key.startswith("must_") and key not in AbstractCheck.threshold_keys:
                self.logs.error(f"Invalid threshold '{key}'. Must be in '{AbstractCheck.threshold_keys}'.")

        return numeric_threshold

    def __create_check(self, check_args: CheckArgs, column_check_factory_classes: list[CheckFactory]) -> Check | None:
        for column_check_factory_class in column_check_factory_classes:
            check = column_check_factory_class.create_check(check_args)
            if check:
                return check

    def __append_scan_warning_and_error_logs(self, scan_logs: soda_core_logs.Logs) -> None:
        level_map = {
            soda_core_logs.LogLevel.ERROR: LogLevel.ERROR,
            soda_core_logs.LogLevel.WARNING: LogLevel.WARNING,
            soda_core_logs.LogLevel.INFO: LogLevel.INFO,
            soda_core_logs.LogLevel.DEBUG: LogLevel.DEBUG,
        }
        for scan_log in scan_logs.logs:
            if scan_log.level in [soda_core_logs.LogLevel.ERROR, soda_core_logs.LogLevel.WARNING]:
                contracts_location: Location = (
                    Location(file_path=self.contract_file.get_file_name(), line=scan_log.location.line, column=scan_log.location.col)
                    if scan_log.location is not None
                    else None
                )
                contracts_level: LogLevel = level_map[scan_log.level]
                self.logs._log(
                    Log(
                        level=contracts_level,
                        message=f"SodaCL: {scan_log.message}",
                        location=contracts_location,
                        exception=scan_log.exception,
                    )
                )

    def verify(self) -> ContractResult:
        scan = Scan()

        scan_logs = soda_core_logs.Logs(logger=scan_logger)
        scan_logs.verbose = True

        sodacl_yaml_str: str | None = None
        try:
            sodacl_yaml_str = self.__generate_sodacl_yaml_str()
            logger.debug(sodacl_yaml_str)

            if sodacl_yaml_str and hasattr(self.data_source, "sodacl_data_source"):
                scan._logs = scan_logs

                # This assumes the connection is a DataSourceConnection
                sodacl_data_source = self.data_source.sodacl_data_source
                # Execute the contract SodaCL in a scan
                scan.set_data_source_name(sodacl_data_source.data_source_name)
                scan_definition_name = (
                    f"dataset://{self.data_source.data_source_name}/{self.schema}/{self.dataset}"
                    if self.schema
                    else f"dataset://{self.data_source.data_source_name}/{self.dataset}"
                )
                # noinspection PyProtectedMember
                scan._data_source_manager.data_sources[self.data_source.data_source_name] = sodacl_data_source

                # if self.soda_cloud:
                #     scan.set_scan_definition_name(scan_definition_name)
                #     scan._configuration.soda_cloud = SodaCloud(
                #         host=self.soda_cloud.host,
                #         api_key_id=self.soda_cloud.api_key_id,
                #         api_key_secret=self.soda_cloud.api_key_secret,
                #         token=self.soda_cloud.token,
                #         port=self.soda_cloud.port,
                #         logs=scan_logs,
                #         scheme=self.soda_cloud.scheme,
                #     )

                if self.variables:
                    scan.add_variables(self.variables)

                scan.add_sodacl_yaml_str(sodacl_yaml_str)
                scan.execute()

        except Exception as e:
            self.logs.error(f"Data contract verification error: {e}", exception=e)

        # The scan warning and error logs are copied into self.logs and at the end of this
        # method, a SodaException is raised if there are error logs.
        self.__append_scan_warning_and_error_logs(scan_logs)

        contract_result: ContractResult = ContractResult(
            contract=self, sodacl_yaml_str=sodacl_yaml_str, logs=self.logs, scan=scan
        )

        return contract_result

    def __generate_sodacl_yaml_str(self) -> str:
        # Serialize the SodaCL YAML object to a YAML string
        sodacl_checks: list = []
        sodacl_yaml_object: dict = (
            {
                f"filter {self.dataset} [filter]": {"where": self.filter_sql},
                f"checks for {self.dataset} [filter]": sodacl_checks,
            }
            if self.filter_sql
            else {f"checks for {self.dataset}": sodacl_checks}
        )

        for check in self.checks:
            if not check.skip:
                sodacl_check = check.to_sodacl_check()
                if sodacl_check is not None:
                    sodacl_checks.append(sodacl_check)
        yaml_helper: YamlHelper = YamlHelper(logs=self.logs)
        return yaml_helper.write_to_yaml_str(sodacl_yaml_object)


@dataclass
class ContractResult:
    """
    This is the immutable data structure containing all the results from a single contract verification.
    This includes any potential execution errors as well as the results of all the checks performed.
    """

    contract: Contract
    sodacl_yaml_str: str | None
    # self.logs combines all the logs of the contract verification with the logs of the Connection parsing,
    # connection usage, SodaCloud parsing and usage (if used) and contract parsing.
    # At the end of the verify method a SodaException is raised if there are any error logs or check failures.
    # See also adr/03_exceptions_vs_error_logs.md
    logs: Logs
    check_results: List[CheckResult]

    def __init__(self, contract: Contract, sodacl_yaml_str: str | None, logs: Logs, scan: Scan):
        self.contract = contract
        self.sodacl_yaml_str = sodacl_yaml_str
        # See also adr/03_exceptions_vs_error_logs.md
        self.logs: Logs = Logs(logs)
        self.check_results: List[CheckResult] = []

        contract_checks_by_id: dict[str, Check] = {check.identity: check for check in contract.checks}

        schema_check: SchemaCheck | None = next((c for c in contract.checks if isinstance(c, SchemaCheck)), None)

        scan_metrics_by_id: dict[str, dict] = {
            scan_metric["identity"]: scan_metric for scan_metric in scan.scan_results.get("metrics", [])
        }

        scan_checks = scan.scan_results.get("checks")
        if isinstance(scan_checks, list):
            for scan_check in scan_checks:
                contract_check: Check | None = None
                if scan_check.get("name") == "Schema Check" and scan_check.get("type") == "generic":
                    contract_check = schema_check
                else:
                    contract_check_id = scan_check.get("contract_check_id")
                    if isinstance(contract_check_id, str):
                        contract_check = contract_checks_by_id[contract_check_id]

                assert contract_check is not None, "Contract scan check matching failed :("

                scan_check_metric_ids = scan_check.get("metrics")
                scan_check_metrics = [
                    scan_metrics_by_id.get(check_metric_id) for check_metric_id in scan_check_metric_ids
                ]
                scan_check_metrics_by_name = {
                    scan_check_metric.get("metricName"): scan_check_metric for scan_check_metric in scan_check_metrics
                }
                check_result = contract_check.create_check_result(
                    scan_check=scan_check, scan_check_metrics_by_name=scan_check_metrics_by_name, scan=scan
                )
                self.check_results.append(check_result)

    def failed(self) -> bool:
        return self.has_execution_errors() or self.has_check_failures()

    def passed(self) -> bool:
        return not self.failed()

    def has_execution_errors(self):
        return self.logs.has_errors()

    def has_check_failures(self):
        return any(check.outcome == CheckOutcome.FAIL for check in self.check_results)

    def __str__(self) -> str:
        error_texts_list: List[str] = [str(error) for error in self.logs.get_errors()]

        check_failure_message_list: list[str] = []
        for check_result in self.check_results:
            if check_result.outcome == CheckOutcome.FAIL:
                result_str_lines = check_result.get_contract_result_str_lines()
                check_failure_message_list.extend(result_str_lines)

        if not error_texts_list and not check_failure_message_list:
            return "All is good. No checks failed. No contract execution errors."

        errors_summary_text = f"{len(error_texts_list)} execution error"
        if len(error_texts_list) != 1:
            errors_summary_text = f"{errors_summary_text}s"

        checks_summary_text = f"{len(check_failure_message_list)} check failure"
        if len(check_failure_message_list) != 1:
            checks_summary_text = f"{checks_summary_text}s"

        parts = [f"{checks_summary_text} and {errors_summary_text}"]
        if error_texts_list:
            error_lines_text: str = indent("\n".join(error_texts_list), "  ")
            parts.append(f"Errors: \n{error_lines_text}")

        if check_failure_message_list:
            parts.append("\n".join(check_failure_message_list))

        return "\n".join(parts)
