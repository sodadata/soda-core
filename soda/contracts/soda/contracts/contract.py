from __future__ import annotations

import logging
from dataclasses import dataclass
from numbers import Number
from textwrap import indent
from typing import List

from soda.cloud.soda_cloud import SodaCloud
from soda.common import logs as soda_core_logs
from soda.scan import logger as scan_logger

from soda.contracts.check import FreshnessCheck, Check, MissingConfigurations, ValidConfigurations, SchemaCheck, \
    NumericThreshold, MissingCheckFactory, InvalidCheckFactory, DuplicateCheckFactory, ValidValuesReferenceData, \
    AbstractCheck, UserDefinedMetricExpressionSqlCheckFactory, SqlFunctionCheckFactory, CheckFactory, CheckResult, \
    CheckOutcome
from soda.contracts.connection import Connection
from soda.contracts.impl.json_schema_verifier import JsonSchemaVerifier
from soda.contracts.impl.logs import Location, Log, LogLevel, Logs
from soda.contracts.impl.variable_resolver import VariableResolver
from soda.contracts.impl.yaml_helper import YamlHelper
from soda.scan import Scan

logger = logging.getLogger(__name__)


class Contract:

    @classmethod
    def from_yaml_str(cls, contract_yaml_str: str) -> Contract:
        return Contract(contract_yaml_str=contract_yaml_str)

    @classmethod
    def from_yaml_file(cls, contract_yaml_file_path: str) -> Contract:
        return Contract(contract_yaml_file_path=contract_yaml_file_path)

    @classmethod
    def from_dict(cls, contract_yaml_dict: dict) -> Contract:
        return Contract(contract_yaml_dict=contract_yaml_dict)

    def __init__(
        self,
        contract_yaml_file_path: str | None = None,
        contract_yaml_str: str | None = None,
        contract_yaml_dict: dict | None = None
    ):
        self.yaml_file_path: str | None = contract_yaml_file_path
        self.yaml_str: str | None = contract_yaml_str
        self.yaml_dict: dict | None = contract_yaml_dict

        self.variables: dict[str, str] = {}
        self.soda_cloud: SodaCloud | None = None
        self.logs: Logs = Logs()
        self.spark_session = None
        self.connection: Connection | None = None

        # TODO decide on file format for data sources: one per file or list per file
        # TODO explain data_source resolving:
        #   - first in ${user.home}/.soda/data_sources/*.yml
        #   - then in a list of identified data source file paths with contract.with_data_source_file_path(file_path)
        #   - then gradually up the dir hierarchy to find higher up data_source.yml files automatically
        self.data_source: str | None = None
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

    def with_variable(self, key: str, value: str) -> Contract:
        self.variables[key] = value
        return self

    def with_variables(self, variables: dict[str, str]) -> Contract:
        if isinstance(variables, dict):
            self.variables.update(variables)
        return self

    def with_soda_cloud(self, soda_cloud: SodaCloud) -> Contract:
        self.soda_cloud = soda_cloud
        return self

    def with_spark_session(self, spark_session) -> Contract:
        self.spark_session = spark_session
        return self

    def with_connection(self, connection: Connection) -> Contract:
        self.connection = connection
        return self

    def with_logs(self, logs: Logs) -> Contract:
        self.logs = logs
        return self

    def verify(self) -> ContractResult:
        self.__parse()
        return self.__execute()

    def __parse(self) -> None:
        try:
            if isinstance(self.yaml_file_path, str):
                with open(self.yaml_file_path) as f:
                    self.yaml_str = f.read()

            if self.variables:
                # Resolve all the ${VARIABLES} in the contract based on either the provided
                # variables or system variables (os.environ)
                variable_resolver = VariableResolver(logs=self.logs, variables=self.variables)
                resolved_contract_yaml_str: str = variable_resolver.resolve(self.yaml_str)
            else:
                resolved_contract_yaml_str = self.yaml_str

            if isinstance(resolved_contract_yaml_str, str):
                from ruamel.yaml import YAML
                ruamel_yaml: YAML = YAML()
                ruamel_yaml.preserve_quotes = True
                self.yaml_dict = ruamel_yaml.load(resolved_contract_yaml_str)

            # Verify the contract schema on the ruamel instance object
            if isinstance(self.yaml_dict, dict):
                json_schema_verifier: JsonSchemaVerifier = JsonSchemaVerifier(self.logs)
                json_schema_verifier.verify(self.yaml_dict)

            yaml_helper = YamlHelper(logs=self.logs)

            self.data_source: str | None = yaml_helper.read_string_opt(self.yaml_dict, "data_source")
            self.schema: str | None = yaml_helper.read_string_opt(self.yaml_dict, "schema")
            self.dataset: str | None = yaml_helper.read_string(self.yaml_dict, "dataset")
            self.filter_sql: str | None = yaml_helper.read_string_opt(
                self.yaml_dict,
                "filter_expression_sql"
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
                verification_context=self.verification_context,
                yaml_contract=self.yaml_dict
            ))

            yaml_columns: list | None = yaml_helper.read_yaml_list(self.yaml_dict, "columns")
            if yaml_columns:
                for yaml_column in yaml_columns:
                    column: str | None = yaml_helper.read_string(yaml_column, "name")
                    check_yamls: list | None = yaml_helper.read_yaml_list_opt(yaml_column, "checks")
                    if column and check_yamls:
                        for check_yaml in check_yamls:
                            check_type: str | None = yaml_helper.read_string(check_yaml, "type")

                            check_name = yaml_helper.read_string(check_yaml,"name")

                            missing_configurations: MissingConfigurations | None = self.__parse_missing_configurations(
                                check_yaml=check_yaml, column=column
                            )
                            valid_configurations: ValidConfigurations | None = self.__parse_valid_configurations(
                                check_yaml=check_yaml, column=column
                            )
                            threshold: NumericThreshold = self.__parse_numeric_threshold(
                                check_yaml=check_yaml
                            )

                            check_args: dict = {
                                "logs": self.logs,
                                "verification_context": self.verification_context,
                                "column": column,
                                "check_type": check_type,
                                "check_name": check_name,
                                "missing_configurations": missing_configurations,
                                "valid_configurations": valid_configurations,
                                "threshold": threshold,
                                "check_yaml": check_yaml
                            }

                            column_check_factory_classes: list[CheckFactory] = [
                                MissingCheckFactory(),
                                InvalidCheckFactory(),
                                DuplicateCheckFactory(),
                                UserDefinedMetricExpressionSqlCheckFactory(),
                                FreshnessCheck(),
                                SqlFunctionCheckFactory(),
                            ]

                            check: Check | None = None
                            for column_check_factory_class in column_check_factory_classes:
                                check = column_check_factory_class.create_check(**check_args)

                            if check:
                                self.checks.append(check)
                            else:
                                location: Location = yaml_helper.create_location_from_yaml_value(check_yaml)
                                self.logs.error(f"Invalid {check_type} check", location=location)

            # TODO check for duplicate identities

        except OSError as e:
            self.logs.error(
                message=f"Could not verify contract: {e}",
                exception=e
            )

    def __parse_missing_configurations(self, check_yaml: dict, column: str) -> MissingConfigurations | None:
        yaml_helper: YamlHelper = YamlHelper(self.logs)
        missing_values: list | None = yaml_helper.read_yaml_list_opt(check_yaml, "missing_values")
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

        invalid_values: list | None = yaml_helper.read_yaml_list_opt(check_yaml, "invalid_values")
        invalid_format: str | None = yaml_helper.read_string_opt(check_yaml, "invalid_format")
        invalid_sql_regex: str | None = yaml_helper.read_string_opt(check_yaml, "invalid_sql_regex")

        valid_values: list | None = yaml_helper.read_yaml_list_opt(check_yaml, "valid_values")

        valid_format: str | None = yaml_helper.read_string_opt(check_yaml, "valid_format")
        valid_sql_regex: str | None = yaml_helper.read_string_opt(check_yaml, "valid_sql_regex")

        valid_min: Number | None = yaml_helper.read_number_opt(check_yaml, "valid_min")
        valid_max: Number | None = yaml_helper.read_number_opt(check_yaml, "valid_max")

        valid_length: int | None = yaml_helper.read_number_opt(check_yaml, "valid_length")
        valid_min_length: int | None = yaml_helper.read_number_opt(check_yaml, "valid_min_length")
        valid_max_length: int | None = yaml_helper.read_number_opt(check_yaml, "valid_max_length")

        valid_values_reference_data: ValidValuesReferenceData | None = None
        valid_values_reference_data_yaml_object: dict | None = yaml_helper.read_yaml_object_opt(
            check_yaml,
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

    def __parse_numeric_threshold(self, check_yaml: dict) -> NumericThreshold | None:
        yaml_helper: YamlHelper = YamlHelper(self.logs)

        numeric_threshold: NumericThreshold = NumericThreshold(
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

    def append_scan_warning_and_error_logs(self, scan_logs: soda_core_logs.Logs) -> None:
        level_map = {
            soda_core_logs.LogLevel.ERROR: LogLevel.ERROR,
            soda_core_logs.LogLevel.WARNING: LogLevel.WARNING,
            soda_core_logs.LogLevel.INFO: LogLevel.INFO,
            soda_core_logs.LogLevel.DEBUG: LogLevel.DEBUG,
        }
        for scan_log in scan_logs.logs:
            if scan_log.level in [soda_core_logs.LogLevel.ERROR, soda_core_logs.LogLevel.WARNING]:
                contracts_location: Location = (
                    Location(line=scan_log.location.line, column=scan_log.location.col)
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

    def __execute(self) -> ContractResult:

        if self.connection is None:
            if self.spark_session is not None:
                self.connection = Connection.from_spark_session(self.spark_session)
            elif isinstance(self.data_source, str):
                self.connection = Connection.from_data_source_name(self.data_source)
            else:
                # TODO provide better diagnostic info and pointers on how to fix this error
                self.logs.error("'data_source' not specified")

        if self.connection:
            pass
        else:
            # TODO provide better diagnostic info and pointers on how to fix this error
            self.logs.error("No connection")

        scan = Scan()

        scan_logs = soda_core_logs.Logs(logger=scan_logger)
        scan_logs.verbose = True

        sodacl_yaml_str: str | None = None
        try:
            sodacl_yaml_str = self.__generate_sodacl_yaml_str()
            logger.debug(sodacl_yaml_str)

            if sodacl_yaml_str and hasattr(self.connection, "data_source"):
                scan._logs = scan_logs

                # This assumes the connection is a DataSourceConnection
                data_source = self.connection.data_source
                # Execute the contract SodaCL in a scan
                scan.set_data_source_name(data_source.data_source_name)
                scan_definition_name = (
                    f"dataset://{self.connection.name}/{self.schema}/{self.dataset}"
                    if self.schema
                    else f"dataset://{self.connection.name}/{self.dataset}"
                )
                scan._data_source_manager.data_sources[data_source.data_source_name] = data_source

                if self.soda_cloud:
                    scan.set_scan_definition_name(scan_definition_name)
                    scan._configuration.soda_cloud = SodaCloud(
                        host=self.soda_cloud.host,
                        api_key_id=self.soda_cloud.api_key_id,
                        api_key_secret=self.soda_cloud.api_key_secret,
                        token=self.soda_cloud.token,
                        port=self.soda_cloud.port,
                        logs=scan_logs,
                        scheme=self.soda_cloud.scheme,
                    )

                if self.variables:
                    scan.add_variables(self.variables)

                # noinspection PyProtectedMember
                scan.add_sodacl_yaml_str(sodacl_yaml_str)
                scan.execute()

        except Exception as e:
            self.logs.error(f"Data contract verification error: {e}", exception=e)

        if self.soda_cloud:
            # If SodaCloud is configured, the logs are copied into the contract result and
            # at the end of this method, a SodaException is raised if there are error logs.
            self.logs.logs.extend(self.soda_cloud.logs.logs)
        if self.connection:
            # The connection logs are copied into the contract result and at the end of this
            # method, a SodaException is raised if there are error logs.
            self.logs.logs.extend(self.connection.logs.logs)
        # The scan warning and error logs are copied into self.logs and at the end of this
        # method, a SodaException is raised if there are error logs.
        self.append_scan_warning_and_error_logs(scan_logs)

        contract_result: ContractResult = ContractResult(
            contract=self, sodacl_yaml_str=sodacl_yaml_str, logs=self.logs, scan=scan
        )
        if contract_result.failed():
            raise SodaException(contract_result=contract_result)

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
        yaml_helper: YamlHelper = YamlHelper(self.logs)
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


class SodaException(Exception):
    """
    See also adr/03_exceptions_vs_error_logs.md
    """

    def __init__(self, message: str | None = None, contract_result: ContractResult | None = None):
        from soda.contracts.contract import ContractResult

        self.contract_result: ContractResult = contract_result
        message_parts: list[str] = []
        if message:
            message_parts.append(message)
        if self.contract_result:
            message_parts.append(str(self.contract_result))
        exception_message: str = "\n".join(message_parts)
        super().__init__(exception_message)
