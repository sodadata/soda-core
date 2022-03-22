from __future__ import annotations

import json
import logging
import os
import textwrap
from datetime import datetime

from soda.__version__ import SODA_CORE_VERSION
from soda.common.log import Log, LogLevel
from soda.common.logs import Logs
from soda.common.undefined_instance import undefined
from soda.execution.check import Check
from soda.execution.check_outcome import CheckOutcome
from soda.execution.data_source_scan import DataSourceScan
from soda.execution.derived_metric import DerivedMetric
from soda.execution.metric import Metric
from soda.soda_cloud.historic_descriptor import HistoricDescriptor
from soda.sodacl.location import Location
from soda.sodacl.sodacl_cfg import SodaCLCfg

logger = logging.getLogger(__name__)


class Scan:
    def __init__(self):
        from soda.configuration.configuration import Configuration
        from soda.execution.check import Check
        from soda.execution.data_source_manager import DataSourceManager
        from soda.execution.query import Query

        now = datetime.utcnow()
        self._logs = Logs(logger)
        self._scan_definition_name: str | None = None
        self._data_source_name: str | None = None
        self._variables: dict[str, object] = {"NOW": now.isoformat()}
        self._configuration: Configuration = Configuration(scan=self)
        self._sodacl_cfg: SodaCLCfg = SodaCLCfg(scan=self)
        self._file_paths: set[str] = set()
        self._data_timestamp: datetime = now
        self._scan_start_timestamp: datetime = now
        self._scan_end_timestamp: datetime = None
        self._data_source_manager = DataSourceManager(self._logs, self._configuration)
        self._data_source_scans: list[DataSourceScan] = []
        self._metrics: set[Metric] = set()
        self._checks: list[Check] = []
        self._queries: list[Query] = []
        self._logs.info(f"Soda Core {SODA_CORE_VERSION}")

    def set_data_source_name(self, data_source_name: str):
        """
        Specifies which datasource to use for the checks.
        """
        self._data_source_name = data_source_name

    def set_scan_definition_name(self, scan_definition_name: str):
        """
        The scan definition name is required if the scan is connected to Soda Cloud in order to correlate subsequent scans from the same pipeline.
        """
        self._scan_definition_name = scan_definition_name

    def set_verbose(self, verbose: bool = True):
        self._logs.verbose = verbose

    def add_configuration_yaml_file(self, file_path: str):
        """
        Configure environment configurations from a YAML file
        """
        try:
            configuration_yaml_str = self._read_file("configuration", file_path)
            self._parse_configuration_yaml_str(
                configuration_yaml_str=configuration_yaml_str,
                file_path=file_path,
            )
        except Exception as e:
            self._logs.error(
                f"Could not add environment configurations from file path {file_path}",
                exception=e,
            )

    def add_configuration_yaml_str(self, environment_yaml_str: str, file_path: str = "yaml string"):
        """
        Configure environment configurations from the given string.
        Parameter file_path is optional and can be used to get the location of the log/error in the logs.
        """
        try:
            self._parse_configuration_yaml_str(
                configuration_yaml_str=environment_yaml_str,
                file_path=file_path,
            )
        except Exception as e:
            self._logs.error(
                f"Could not add environment configurations from string",
                exception=e,
            )

    def _parse_configuration_yaml_str(self, configuration_yaml_str: str, file_path: str = "yaml string"):
        from soda.configuration.configuration_parser import ConfigurationParser

        environment_parse = ConfigurationParser(
            configuration=self._configuration,
            logs=self._logs,
            file_path=file_path,
        )
        environment_parse.parse_environment_yaml_str(configuration_yaml_str)

    def add_configuration_spark_session(self, data_source_name: str, spark_session):
        """
        Pass a spark_session to the scan.  Only required in case of PySpark scans.
        """
        try:
            self._configuration.add_spark_session(data_source_name=data_source_name, spark_session=spark_session)
        except Exception as e:
            self._logs.error(
                f"Could not add environment spark session for data_source {data_source_name}",
                exception=e,
            )

    def add_sodacl_yaml_files(
        self,
        path: str,
        recursive: bool | None = True,
        suffix: str | None = ".yml",
    ):
        """
        Adds all the files in the given directory to the scan as SodaCL files.
        Parameter 'path' is a string that typically represents a directory, but it can also be a SodaCL file.
                         ~ will be expanded to the user home dir the directory in which to search for SodaCL files.
        Parameter 'recursive' controls if nested directories also will be scanned.  Default recursive=True.
        Parameter 'suffix' can be used to only load files having a given extension or suffix like eg suffix='.sodacl.yml'
        """
        try:
            if isinstance(path, str):
                file_system = self._configuration.file_system
                path = file_system.expand_user(path)
                if file_system.exists(path):
                    if file_system.is_dir(path):
                        self._logs.info(f"Adding SodaCL dir {path}")
                        for dir_entry in file_system.scan_dir(path):
                            if dir_entry.is_file() and (suffix is None or dir_entry.name.endswith(suffix)):
                                self.add_sodacl_yaml_file(file_path=dir_entry.path)
                            elif recursive and dir_entry.is_dir():
                                self.add_sodacl_yaml_files(
                                    path=dir_entry.path,
                                    recursive=True,
                                    suffix=suffix,
                                )
                    elif file_system.is_file(path):
                        self.add_sodacl_yaml_file(file_path=path)
                    else:
                        self._logs.error(f'path "{path}" exists, but is not a file nor directory ?!')
                else:
                    self._logs.error(f'path "{path}" does not exist')
            else:
                self._logs.error(f"path is not a string: {type(path).__name__}")
        except Exception as e:
            self._logs.error(f"Could not add SodaCL files from dir {dir}", exception=e)

    def add_sodacl_yaml_file(self, file_path: str):
        """
        Add a SodaCL file to the scan.
        """
        try:
            sodacl_yaml_str = self._read_file("SodaCL", file_path)
            if file_path not in self._file_paths:
                self._file_paths.add(file_path)
                self._parse_sodacl_yaml_str(sodacl_yaml_str=sodacl_yaml_str, file_path=file_path)
            else:
                self._logs.debug(f"Skipping duplicate file addition for {file_path}")
        except Exception as e:
            self._logs.error(f"Could not add SodaCL file {file_path}", exception=e)

    def add_sodacl_yaml_str(self, sodacl_yaml_str: str):
        """
        Add a SodaCL string to the scan.
        """
        try:
            unique_name = "sodacl_string"
            if unique_name in self._file_paths:
                number: int = 2
                while f"{unique_name}_{number}" in self._file_paths:
                    number += 1
                unique_name = f"{unique_name}_{number}"
            file_path = f"{unique_name}.yml"
            self._parse_sodacl_yaml_str(sodacl_yaml_str=sodacl_yaml_str, file_path=file_path)
        except Exception as e:
            self._logs.error(f"Could not add SodaCL string", exception=e)

    def _parse_sodacl_yaml_str(self, sodacl_yaml_str: str, file_path: str = None):
        from soda.sodacl.sodacl_parser import SodaCLParser

        sodacl_parser = SodaCLParser(
            sodacl_cfg=self._sodacl_cfg,
            logs=self._logs,
            file_path=file_path,
            data_source_name=self._data_source_name,
        )
        sodacl_parser.parse_sodacl_yaml_str(sodacl_yaml_str)

    def _read_file(self, file_type: str, file_path: str) -> str:
        file_location = Location(file_path)
        file_system = self._configuration.file_system
        resolved_file_path = file_system.expand_user(file_path)
        if not file_system.exists(resolved_file_path):
            self._logs.error(
                f"File {resolved_file_path} does not exist",
                location=file_location,
            )
            return None
        if file_system.is_dir(resolved_file_path):
            self._logs.error(
                f"File {resolved_file_path} exists, but is a directory",
                location=file_location,
            )
            return None
        try:
            self._logs.debug(f'Reading {file_type} file "{resolved_file_path}"')
            file_content_str = file_system.file_read_as_str(resolved_file_path)
            if not isinstance(file_content_str, str):
                self._logs.error(
                    f"Error reading file {resolved_file_path} from the file system",
                    location=file_location,
                )
            return file_content_str
        except Exception as e:
            self._logs.error(
                f"Error reading file {resolved_file_path} from the file system",
                location=file_location,
                exception=e,
            )

    def add_variables(self, variables: dict[str, str]):
        """
        Add variables to the scan. Keys and values must be strings.
        """
        try:
            self._variables.update(variables)
        except Exception as e:
            variables_text = json.dumps(variables)
            self._logs.error(f"Could not add variables {variables_text}", exception=e)

    def disable_telemetry(self):
        """
        Disables all telemetry.  For more information see Soda's public statements on telemetry.  TODO add links.
        """
        self._configuration.telemetry = None

    def execute(self):
        self._logs.debug("Scan execution starts")
        try:
            from soda.execution.column import Column
            from soda.execution.column_metrics import ColumnMetrics
            from soda.execution.partition import Partition
            from soda.execution.table import Table
            from soda.soda_cloud.historic_descriptor import HistoricDescriptor

            # If there is a sampler
            if self._configuration.sampler:
                # ensure the sampler is configured with the scan logs
                self._configuration.sampler.logs = self._logs

            # Resolve the for each table checks and add them to the scan_cfg data structures
            self.__resolve_for_each_table_checks()
            # Resolve the for each column checks and add them to the scan_cfg data structures
            self.__resolve_for_each_column_checks()

            # For each data_source, build up the DataSourceScan data structures
            for data_source_scan_cfg in self._sodacl_cfg.data_source_scan_cfgs.values():
                # This builds up the data structures that correspond to the cfg model
                data_source_scan = self._get_or_create_data_source_scan(data_source_scan_cfg.data_source_name)
                if data_source_scan:
                    for check_cfg in data_source_scan_cfg.check_cfgs:
                        self.__create_check(check_cfg, data_source_scan)

                    for table_cfg in data_source_scan_cfg.tables_cfgs.values():
                        table: Table = data_source_scan.get_or_create_table(table_cfg.table_name)

                        for column_configurations_cfg in table_cfg.column_configurations_cfgs.values():
                            column: Column = table.get_or_create_column(column_configurations_cfg.column_name)
                            column.set_column_configuration_cfg(column_configurations_cfg)

                        for partition_cfg in table_cfg.partition_cfgs:
                            partition: Partition = table.get_or_create_partition(partition_cfg.partition_name)
                            partition.set_partition_cfg(partition_cfg)

                            for check_cfg in partition_cfg.check_cfgs:
                                self.__create_check(check_cfg, data_source_scan, partition)

                            if partition_cfg.column_checks_cfgs:
                                for column_checks_cfg in partition_cfg.column_checks_cfgs.values():
                                    column_metrics: ColumnMetrics = partition.get_or_create_column_metrics(
                                        column_checks_cfg.column_name
                                    )
                                    column_metrics.set_column_check_cfg(column_checks_cfg)
                                    if column_checks_cfg.check_cfgs:
                                        for check_cfg in column_checks_cfg.check_cfgs:
                                            self.__create_check(
                                                check_cfg,
                                                data_source_scan,
                                                partition,
                                                column_metrics.column,
                                            )

            # Each data_source is asked to create metric values that are returned as a list of query results
            for data_source_scan in self._data_source_scans:
                data_source_scan.execute_queries()

            # Compute derived metric values
            for metric in self._metrics:
                if isinstance(metric, DerivedMetric):
                    metric.compute_derived_metric_values()

            # Collect the metric store requests from the checks and fetch them from Soda Cloud
            historic_descriptors: set[HistoricDescriptor] = set()
            for check in self._checks:
                if check.historic_descriptors:
                    historic_descriptors.update(check.historic_descriptors.values())
            if historic_descriptors:
                historic_data = self.__get_historic_data_from_soda_cloud_metric_store(historic_descriptors)

            # Evaluates the checks based on all the metric values
            for check in self._checks:
                # First get the metric values for this check
                check_metrics = {}
                missing_value_metrics = []
                for check_metric_name, metric in check.metrics.items():
                    if metric.value is not undefined:
                        check_metrics[check_metric_name] = metric
                    else:
                        missing_value_metrics.append(metric)

                # Get historic data for this check
                check_historic_values = {}
                if check.historic_descriptors:
                    for (
                        historic_name,
                        historic_descriptor,
                    ) in check.historic_descriptors.items():
                        historic_value = historic_data.get(historic_descriptor)
                        check_historic_values[historic_name] = historic_value

                if not missing_value_metrics:
                    try:
                        check.evaluate(check_metrics, check_historic_values)
                    except BaseException as e:
                        self._logs.error(
                            f"Evaluation of check {check.check_cfg.source_line} failed: {e}",
                            location=check.check_cfg.location,
                            exception=e,
                        )
                else:
                    missing_metrics_str = ",".join([str(metric) for metric in missing_value_metrics])
                    self._logs.error(
                        f"Metrics {missing_metrics_str} were not computed for check {check.check_cfg.source_line}"
                    )

            # TODO Show the results on the console: check results, metric values and queries
            # TODO Send the results to Soda Cloud

            for data_source_scan in self._data_source_scans:
                for monitoring_cfg in data_source_scan.data_source_scan_cfg.monitoring_cfgs:
                    data_source_name = data_source_scan.data_source_scan_cfg.data_source_name
                    data_source_scan = self._get_or_create_data_source_scan(data_source_name)
                    if data_source_scan:
                        monitor_runner = data_source_scan.create_automated_monitor_run(monitoring_cfg, self)
                        monitor_runner.run()
                    else:
                        data_source_names = ", ".join(self._data_source_manager.data_source_properties_by_name.keys())
                        self._logs.error(
                            f"Could not run monitors on data_source {data_source_name} because It is not "
                            f"configured: {data_source_names}"
                        )

            self._logs.info("Scan summary:")
            self.__log_queries(having_exception=False)
            self.__log_queries(having_exception=True)

            checks_pass_count = self.__log_checks(CheckOutcome.PASS)
            checks_warn_count = self.__log_checks(CheckOutcome.WARN)
            warn_text = "warning" if checks_warn_count == 1 else "warnings"
            checks_fail_count = self.__log_checks(CheckOutcome.FAIL)
            fail_text = "failure" if checks_warn_count == 1 else "failures"
            error_count = len(self.get_error_logs())
            error_text = "error" if error_count == 1 else "errors"
            self.__log_checks(None)
            checks_not_evaluated = len(self._checks) - checks_pass_count - checks_warn_count - checks_fail_count

            if checks_not_evaluated:
                self._logs.info(f"{checks_not_evaluated} checks not evaluated.")
            if error_count > 0:
                self._logs.info(f"{error_count} errors.")
            if checks_warn_count + checks_fail_count + error_count == 0:
                if checks_not_evaluated:
                    self._logs.info(
                        f"Apart from the checks that have not been evaluated, no failures, no warnings and no errors."
                    )
                else:
                    self._logs.info(f"All is good. No failures. No warnings. No errors.")
            elif checks_fail_count > 0:
                self._logs.info(
                    f"Oops! {checks_fail_count} {fail_text}. {checks_warn_count} {warn_text}. {error_count} {error_text}. {checks_pass_count} pass."
                )
            elif checks_warn_count > 0:
                self._logs.info(
                    f"Only {checks_warn_count} {warn_text}. {checks_fail_count} {fail_text}. {error_count} {error_text}. {checks_pass_count} pass."
                )
            elif error_count > 0:
                self._logs.info(
                    f"Oops! {error_count} {error_text}. {checks_fail_count} {fail_text}. {checks_warn_count} {warn_text}. {checks_pass_count} pass."
                )

            if error_count > 0:
                Log.log_errors(self.get_error_logs())

            self._scan_end_timestamp = datetime.now()
            if self._configuration.soda_cloud:
                self._logs.info("Sending results to Soda Cloud")
                self._configuration.soda_cloud.send_scan_results(self)

        except Exception as e:
            self._logs.error(f"Unknown error while executing scan.", exception=e)
        finally:
            self._close()

    def __checks_to_text(self, checks: list[Check]):
        return "/n".join([str(check) for check in checks])

    def _close(self):
        self._data_source_manager.close_all_connections()

    def __create_check(self, check_cfg, data_source_scan=None, partition=None, column=None):
        from soda.execution.check import Check

        check = Check.create(
            check_cfg=check_cfg,
            data_source_scan=data_source_scan,
            partition=partition,
            column=column,
        )
        self._checks.append(check)

    def __resolve_for_each_table_checks(self):
        from soda.execution.query import Query

        data_source_name = self._data_source_name

        for index, for_each_table_cfg in enumerate(self._sodacl_cfg.for_each_table_cfgs):
            include_tables = [include.table_name_filter for include in for_each_table_cfg.includes]
            exclude_tables = [include.table_name_filter for include in for_each_table_cfg.excludes]

            data_source_scan = self._get_or_create_data_source_scan(data_source_name)
            if data_source_scan:
                query_name = f"for_each_table_{for_each_table_cfg.table_alias_name}[{index}]"
                sql = data_source_scan.data_source.sql_find_table_names_includes_excludes(
                    include_tables, exclude_tables
                )
                query = Query(data_source_scan=data_source_scan, unqualified_query_name=query_name, sql=sql)
                query.execute()
                table_names = [row[0] for row in query.rows]

                for table_name in table_names:
                    data_source_scan_cfg = self._sodacl_cfg._get_or_create_data_source_scan_cfgs(data_source_name)
                    table_cfg = data_source_scan_cfg.get_or_create_table_cfg(table_name)
                    partition_cfg = table_cfg.find_partition(None, None)
                    for check_cfg in for_each_table_cfg.check_cfgs:
                        column_name = check_cfg.get_column_name()
                        if column_name:
                            column_checks_cfg = partition_cfg.get_or_create_column_checks(column_name)
                            column_checks_cfg.add_check_cfg(check_cfg)
                        else:
                            partition_cfg.add_check_cfg(check_cfg)

    def __resolve_for_each_column_checks(self):
        if self._sodacl_cfg.for_each_column_cfgs:
            raise NotImplementedError("TODO")

    def _get_or_create_data_source_scan(self, data_source_name: str) -> DataSourceScan:
        from soda.execution.data_source import DataSource
        from soda.sodacl.data_source_scan_cfg import DataSourceScanCfg

        data_source_scan = next(
            (
                data_source_scan
                for data_source_scan in self._data_source_scans
                if data_source_scan.data_source.data_source_name == data_source_name
            ),
            None,
        )
        if data_source_scan is None:
            data_source_scan_cfg = self._sodacl_cfg.data_source_scan_cfgs.get(data_source_name)
            if data_source_scan_cfg is None:
                data_source_scan_cfg = DataSourceScanCfg(data_source_name)
            data_source_name = data_source_scan_cfg.data_source_name
            data_source: DataSource = self._data_source_manager.get_data_source(data_source_name)
            if data_source:
                data_source_scan = data_source.create_data_source_scan(self, data_source_scan_cfg)
                self._data_source_scans.append(data_source_scan)
            else:
                self._sodacl_cfg.data_source_scan_cfgs.pop(data_source_name)
        return data_source_scan

    def _jinja_resolve(
        self,
        definition: str,
        variables: dict[str, object] = None,
        location: Location | None = None,
    ):
        if isinstance(definition, str) and "${" in definition:
            from soda.common.jinja import Jinja

            jinja_variables = self._variables.copy()
            if isinstance(variables, dict):
                jinja_variables.update(variables)
            try:
                return Jinja.resolve(definition, jinja_variables)
            except BaseException as e:
                self._logs.error(
                    message=f"Error resolving Jinja template {definition}: {e}",
                    location=location,
                    exception=e,
                )
        else:
            return definition

    def __get_historic_data_from_soda_cloud_metric_store(
        self, historic_descriptors: set[HistoricDescriptor]
    ) -> dict[HistoricDescriptor, object]:
        if self._configuration.soda_cloud:
            return self._configuration.soda_cloud.get_historic_data(historic_descriptors)
        else:
            self._logs.error("Soda Core must be configured to connect to Soda Cloud to use change-over-time checks.")
        return {}

    def _find_existing_metric(self, metric) -> Metric:
        return next(
            (existing_metric for existing_metric in self._metrics if existing_metric == metric),
            None,
        )

    def _add_metric(self, metric):
        self._metrics.add(metric)

    def __log_queries(self, having_exception: bool) -> int:
        count = sum((query.exception is None) != having_exception for query in self._queries)
        if count > 0:
            status_text = "ERROR" if having_exception else "OK"
            queries_text = "query" if len(self._queries) == 1 else "queries"
            self._logs.debug(f"{count}/{len(self._queries)} {queries_text} {status_text}")
            for query in self._queries:
                query_text = f"\n{query.sql}" if query.exception else ""
                self._logs.debug(f"  {query.query_name} [{status_text}] {query.duration}{query_text}")
                if query.exception:
                    exception_str = str(query.exception)
                    exception_str = textwrap.indent(text=exception_str, prefix="    ")
                    self._logs.debug(exception_str)
        return count

    def __log_checks(self, check_outcome: CheckOutcome | None) -> int:
        count = sum(check.outcome == check_outcome for check in self._checks)
        if count > 0:
            outcome_text = "NOT EVALUATED" if check_outcome is None else f"{check_outcome.value.upper()}ED"
            checks_text = "check" if len(self._checks) == 1 else "checks"
            self._logs.info(f"{count}/{len(self._checks)} {checks_text} {outcome_text}: ")

            checks_by_partition = {}
            other_checks = []
            for check in self._checks:
                if check.outcome == check_outcome:
                    partition = check.partition
                    if partition:
                        partition_name = f" [{partition.partition_name}]" if partition.partition_name else ""
                        partition_title = f"{partition.table.table_name}{partition_name} in {partition.data_source_scan.data_source.data_source_name}"
                        checks_by_partition.setdefault(partition_title, []).append(check)
                    else:
                        other_checks.append(check)

            for (
                partition_title,
                partition_checks,
            ) in checks_by_partition.items():
                if len(partition_checks) > 0:
                    self._logs.info(f"    {partition_title}")
                    self.__log_check_group(partition_checks, "      ", check_outcome, outcome_text)
            if len(other_checks) > 0:
                self.__log_check_group(other_checks, "    ", check_outcome, outcome_text)
        return count

    def __log_check_group(self, checks, indent, check_outcome, outcome_text):
        for check in checks:
            self._logs.info(f"{indent}{check.get_summary()} [{outcome_text}]")
            if self._logs.verbose or check_outcome != CheckOutcome.PASS:
                for diagnostic in check.get_log_diagnostic_lines():
                    self._logs.info(f"{indent}  {diagnostic}")

    def get_variable(self, variable_name: str, default_value: str | None = None) -> str | None:
        # Note: ordering here must be the same as in Jinja.OsContext.resolve_or_missing: First env vars, then scan vars
        if variable_name in os.environ:
            return os.environ[variable_name]
        elif variable_name in self._variables:
            return self._variables[variable_name]
        return default_value

    def get_logs_text(self) -> str | None:
        return self.__logs_to_text(self._logs.logs)

    def has_error_logs(self) -> bool:
        return any(log.level == LogLevel.ERROR for log in self._logs.logs)

    def get_error_logs(self) -> list[Log]:
        return [log for log in self._logs.logs if log.level == LogLevel.ERROR]

    def get_error_logs_text(self) -> str | None:
        return self.__logs_to_text(self.get_error_logs())

    def assert_no_error_logs(self) -> None:
        if self.has_error_logs():
            raise AssertionError(self.get_error_logs_text())

    def has_error_or_warning_logs(self) -> bool:
        return any(log.level in [LogLevel.ERROR, LogLevel.WARNING] for log in self._logs.logs)

    def get_error_or_warning_logs(self) -> list[Log]:
        return [log for log in self._logs.logs if log.level in [LogLevel.ERROR, LogLevel.WARNING]]

    def get_error_or_warning_logs_text(self) -> str | None:
        return self.__logs_to_text(self.get_error_or_warning_logs())

    def assert_no_error_nor_warning_logs(self) -> None:
        if self.has_error_or_warning_logs():
            raise AssertionError(self.get_logs_text())

    def assert_has_error(self, expected_error_message: str):
        if all([expected_error_message not in log.message for log in self.get_error_logs()]):
            raise AssertionError(
                f'Expected error message "{expected_error_message}" did not occur in the error logs:\n{self.get_logs_text()}'
            )

    def __logs_to_text(self, logs: list[Log]):
        if len(logs) == 0:
            return None
        return "\n".join([str(log) for log in logs])

    def has_check_fails(self) -> bool:
        for check in self._checks:
            if check.outcome == CheckOutcome.FAIL:
                return True
        return False

    def has_check_warns(self) -> bool:
        for check in self._checks:
            if check.outcome == CheckOutcome.WARN:
                return True
        return False

    def has_check_warns_or_fails(self) -> bool:
        for check in self._checks:
            if check.outcome in [CheckOutcome.FAIL, CheckOutcome.WARN]:
                return True
        return False

    def assert_no_checks_fail(self):
        if len(self.get_checks_fail()):
            raise AssertionError(f"Check results failed: \n{self.get_checks_fail_text()}")

    def get_checks_fail(self) -> list[Check]:
        return [check for check in self._checks if check.outcome == CheckOutcome.FAIL]

    def get_checks_fail_text(self) -> str | None:
        return self.__checks_to_text(self.get_checks_fail())

    def assert_no_checks_warn_or_fail(self):
        if len(self.get_checks_warn_or_fail()):
            raise AssertionError(f"Check results having warn or fail outcome: \n{self.get_checks_warn_or_fail_text()}")

    def get_checks_warn_or_fail(self) -> list[Check]:
        return [check for check in self._checks if check.outcome in [CheckOutcome.WARN, CheckOutcome.FAIL]]

    def has_checks_warn_or_fail(self) -> bool:
        return len(self.get_checks_warn_or_fail()) > 0

    def get_checks_warn_or_fail_text(self) -> str | None:
        return self.__checks_to_text(self.get_checks_warn_or_fail())

    def get_all_checks_text(self) -> str | None:
        return self.__checks_to_text(self._checks)

    def has_soda_cloud_connection(self):
        return self._configuration.soda_cloud is not None
