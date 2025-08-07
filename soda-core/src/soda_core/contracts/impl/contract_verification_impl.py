from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import timezone
from enum import Enum
from io import StringIO
from logging import LogRecord

from ruamel.yaml import YAML
from soda_core.common.consistent_hash_builder import ConsistentHashBuilder
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.exceptions import InvalidRegexException, SodaCoreException
from soda_core.common.logging_constants import Emoticons, ExtraKeys, soda_logger
from soda_core.common.logs import Location, Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.sql_dialect import *
from soda_core.common.yaml import (
    ContractYamlSource,
    DataSourceYamlSource,
    SodaCloudYamlSource,
)
from soda_core.contracts.contract_verification import (
    Check,
    CheckOutcome,
    CheckResult,
    Contract,
    ContractVerificationResult,
    ContractVerificationSessionResult,
    ContractVerificationStatus,
    DataSource,
    Measurement,
    SodaException,
    Threshold,
    YamlFileContentInfo,
)
from soda_core.contracts.impl.contract_yaml import (
    CheckYaml,
    ColumnYaml,
    ContractYaml,
    MissingAncValidityCheckYaml,
    MissingAndValidityYaml,
    RegexFormat,
    ThresholdYaml,
    ValidReferenceDataYaml,
)
from tabulate import tabulate

logger: logging.Logger = soda_logger


class ContractVerificationHandler:
    @classmethod
    def instance(cls, identifier: Optional[str] = None) -> Optional[ContractVerificationHandler]:
        # TODO: replace with plugin extension mechanism
        try:
            from soda.failed_rows_extractor.failed_rows_extractor import (
                FailedRowsExtractor,
            )

            return FailedRowsExtractor.create()
        except (AttributeError, ModuleNotFoundError) as e:
            # Extension not installed
            return None

    def handle(
        self,
        contract_impl: ContractImpl,
        data_source_impl: Optional[DataSourceImpl],
        contract_verification_result: ContractVerificationResult,
        soda_cloud: SodaCloud,
        soda_cloud_send_results_response_json: dict,
        dwh_data_source_file_path: Optional[str] = None,
    ):
        pass


class ContractVerificationSessionImpl:
    @classmethod
    def execute(
        cls,
        contract_yaml_sources: list[ContractYamlSource],
        only_validate_without_execute: bool = False,
        variables: Optional[dict[str, str]] = None,
        data_timestamp: Optional[str] = None,
        data_source_impls: Optional[list[DataSourceImpl]] = None,
        data_source_yaml_sources: Optional[list[DataSourceYamlSource]] = None,
        soda_cloud_impl: Optional[SodaCloud] = None,
        soda_cloud_publish_results: bool = False,
        soda_cloud_use_agent: bool = False,
        soda_cloud_verbose: bool = False,
        soda_cloud_use_agent_blocking_timeout_in_minutes: int = 60,
        dwh_data_source_file_path: Optional[str] = None,
    ):
        # Start capturing logs
        logs: Logs = Logs()

        # Validate input contract_yaml_sources
        assert isinstance(contract_yaml_sources, list)
        assert all(
            isinstance(contract_yaml_source, ContractYamlSource) for contract_yaml_source in contract_yaml_sources
        )

        # Validate input variables
        if variables is None:
            variables = {}
        else:
            assert isinstance(variables, dict)
            assert all(isinstance(k, str) and isinstance(v, str) for k, v in variables.items())

        # Validate input data_source_impls
        if data_source_impls is None:
            data_source_impls = []
        else:
            assert isinstance(data_source_impls, list)
            assert all(isinstance(data_source_impl, DataSourceImpl) for data_source_impl in data_source_impls)

        # Validate input data_source_yaml_sources
        if data_source_yaml_sources is None:
            data_source_yaml_sources = []
        else:
            assert isinstance(data_source_yaml_sources, list)
            assert all(
                isinstance(data_source_yaml_source, DataSourceYamlSource) or soda_cloud_use_agent
                for data_source_yaml_source in data_source_yaml_sources
            )

        # Validate input soda_cloud_impl
        if soda_cloud_impl is not None:
            assert isinstance(soda_cloud_impl, SodaCloud)

        # Validate input soda_cloud_skip_publish
        assert isinstance(soda_cloud_publish_results, bool)

        # Validate input soda_cloud_use_agent
        assert isinstance(soda_cloud_use_agent, bool)

        # Validate input soda_cloud_use_agent_blocking_timeout_in_minutes
        assert isinstance(soda_cloud_use_agent_blocking_timeout_in_minutes, int)

        if soda_cloud_use_agent:
            contract_verification_results: list[ContractVerificationResult] = cls._execute_on_agent(
                contract_yaml_sources=contract_yaml_sources,
                variables=variables,
                soda_cloud_impl=soda_cloud_impl,
                soda_cloud_use_agent_blocking_timeout_in_minutes=soda_cloud_use_agent_blocking_timeout_in_minutes,
                soda_cloud_publish_results=soda_cloud_publish_results,
                soda_cloud_verbose=soda_cloud_verbose,
            )

        else:
            contract_verification_results: list[ContractVerificationResult] = cls._execute_locally(
                logs=logs,
                contract_yaml_sources=contract_yaml_sources,
                only_validate_without_execute=only_validate_without_execute,
                provided_variable_values=variables,
                data_timestamp=data_timestamp,
                data_source_impls=data_source_impls,
                data_source_yaml_sources=data_source_yaml_sources,
                soda_cloud_impl=soda_cloud_impl,
                soda_cloud_publish_results=soda_cloud_publish_results,
                dwh_data_source_file_path=dwh_data_source_file_path,
            )
        return ContractVerificationSessionResult(contract_verification_results=contract_verification_results)

    @classmethod
    def _execute_locally(
        cls,
        logs: Logs,
        contract_yaml_sources: list[ContractYamlSource],
        only_validate_without_execute: bool,
        provided_variable_values: dict[str, str],
        data_timestamp: Optional[str],
        data_source_impls: list[DataSourceImpl],
        data_source_yaml_sources: list[DataSourceYamlSource],
        soda_cloud_impl: Optional[SodaCloud],
        soda_cloud_publish_results: bool,
        dwh_data_source_file_path: Optional[str] = None,
    ) -> list[ContractVerificationResult]:
        contract_verification_results: list[ContractVerificationResult] = []

        data_source_impls_by_name: dict[str, DataSourceImpl] = cls._build_data_source_impls_by_name(
            data_source_impls=data_source_impls,
            data_source_yaml_sources=data_source_yaml_sources,
            provided_variable_values=provided_variable_values,
        )

        opened_data_sources: list[DataSourceImpl] = []
        try:
            for contract_yaml_source in contract_yaml_sources:
                try:
                    contract_yaml: ContractYaml = ContractYaml.parse(
                        contract_yaml_source=contract_yaml_source,
                        provided_variable_values=provided_variable_values,
                        data_timestamp=data_timestamp,
                    )
                    data_source_name: str = (
                        contract_yaml.dataset[: contract_yaml.dataset.find("/")] if contract_yaml.dataset else None
                    )
                    data_source_impl: Optional[DataSourceImpl] = (
                        cls._get_data_source_impl(data_source_name, data_source_impls_by_name, opened_data_sources)
                        if (contract_yaml and data_source_name and not only_validate_without_execute)
                        else None
                    )
                    contract_impl: ContractImpl = ContractImpl(
                        contract_yaml=contract_yaml,
                        only_validate_without_execute=only_validate_without_execute,
                        data_timestamp=contract_yaml.data_timestamp,
                        execution_timestamp=contract_yaml.execution_timestamp,
                        data_source_impl=data_source_impl,
                        soda_cloud=soda_cloud_impl,
                        publish_results=soda_cloud_publish_results,
                        logs=logs,
                        dwh_data_source_file_path=dwh_data_source_file_path,
                    )
                    contract_verification_result: ContractVerificationResult = contract_impl.verify()
                    contract_verification_results.append(contract_verification_result)
                except Exception:
                    raise
        finally:
            for data_source_impl in opened_data_sources:
                data_source_impl.close_connection()
        return contract_verification_results

    @classmethod
    def _build_data_source_impls_by_name(
        cls,
        data_source_impls: list[DataSourceImpl],
        data_source_yaml_sources: list[DataSourceYamlSource],
        provided_variable_values: dict[str, str],
    ) -> dict[str, DataSourceImpl]:
        data_source_impl_by_name: dict[str, DataSourceImpl] = (
            {data_source_impl.name: data_source_impl for data_source_impl in data_source_impls}
            if data_source_impls
            else {}
        )
        for data_source_yaml_source in data_source_yaml_sources:
            data_source_impl: DataSourceImpl = DataSourceImpl.from_yaml_source(
                data_source_yaml_source=data_source_yaml_source, provided_variable_values=provided_variable_values
            )
            data_source_impl_by_name[data_source_impl.name] = data_source_impl
        return data_source_impl_by_name

    @classmethod
    def _build_soda_cloud_impl(
        cls,
        soda_cloud_impl: Optional[SodaCloud],
        soda_cloud_yaml_source: Optional[SodaCloudYamlSource],
        provided_variable_values: dict[str, str],
    ) -> Optional[SodaCloud]:
        if soda_cloud_impl:
            return soda_cloud_impl
        if soda_cloud_yaml_source:
            return SodaCloud.from_yaml_source(
                soda_cloud_yaml_source=soda_cloud_yaml_source, provided_variable_values=provided_variable_values
            )
        return None

    @classmethod
    def _get_data_source_impl(
        cls,
        data_source_name: Optional[str],
        data_source_impl_by_name: dict[str, DataSourceImpl],
        opened_data_sources: list[DataSourceImpl],
    ) -> Optional[DataSourceImpl]:
        if data_source_name is None:
            return None
        data_source_impl: Optional[DataSourceImpl] = data_source_impl_by_name.get(data_source_name)
        if isinstance(data_source_impl, DataSourceImpl):
            if not data_source_impl.has_open_connection():
                data_source_impl.open_connection()
                opened_data_sources.append(data_source_impl)
            return data_source_impl
        else:
            logger.error(f"Data source '{data_source_name}' not found")

    @classmethod
    def _execute_on_agent(
        cls,
        contract_yaml_sources: list[ContractYamlSource],
        variables: dict[str, str],
        soda_cloud_impl: Optional[SodaCloud],
        soda_cloud_use_agent_blocking_timeout_in_minutes: int,
        soda_cloud_publish_results: bool,
        soda_cloud_verbose: bool,
    ) -> list[ContractVerificationResult]:
        contract_verification_results: list[ContractVerificationResult] = []

        for contract_yaml_source in contract_yaml_sources:
            try:
                contract_yaml: ContractYaml = ContractYaml.parse(
                    contract_yaml_source=contract_yaml_source, provided_variable_values=variables
                )
                contract_verification_result: ContractVerificationResult = soda_cloud_impl.verify_contract_on_agent(
                    contract_yaml=contract_yaml,
                    variables=variables,
                    blocking_timeout_in_minutes=soda_cloud_use_agent_blocking_timeout_in_minutes,
                    publish_results=soda_cloud_publish_results,
                    verbose=soda_cloud_verbose,
                )
                contract_verification_results.append(contract_verification_result)
            except:
                logger.error(msg=f"Could not verify contract {contract_yaml_source}", exc_info=True)
        return contract_verification_results


class ContractImpl:
    def __init__(
        self,
        logs: Logs,
        contract_yaml: ContractYaml,
        only_validate_without_execute: bool,
        data_source_impl: Optional[DataSourceImpl],
        data_timestamp: datetime,
        execution_timestamp: datetime,
        soda_cloud: Optional[SodaCloud],
        publish_results: bool,
        dwh_data_source_file_path: Optional[str] = None,
    ):
        self.logs: Logs = logs
        self.contract_yaml: ContractYaml = contract_yaml
        self.only_validate_without_execute: bool = only_validate_without_execute
        self.data_source_impl: DataSourceImpl = data_source_impl
        self.soda_cloud: Optional[SodaCloud] = soda_cloud
        self.publish_results: bool = publish_results

        self.filter: Optional[str] = self.contract_yaml.filter

        self.started_timestamp: datetime = datetime.now(tz=timezone.utc)

        self.execution_timestamp: datetime = execution_timestamp
        self.data_timestamp: datetime = data_timestamp

        self.dataset_name: Optional[str] = None

        self.check_attributes: dict[str, any] = contract_yaml.check_attributes

        dataset_identifier = DatasetIdentifier.parse(contract_yaml.dataset)
        self.dataset_prefix: list[str] = dataset_identifier.prefixes
        self.dataset_name = dataset_identifier.dataset_name

        self.metrics_resolver: MetricsResolver = MetricsResolver()

        self.column_impls: list[ColumnImpl] = []
        self.check_impls: list[CheckImpl] = []

        # TODO replace usage of self.soda_qualified_dataset_name with self.dataset_identifier
        self.soda_qualified_dataset_name = contract_yaml.dataset
        # TODO replace usage of self.sql_qualified_dataset_name with self.dataset_identifier
        self.sql_qualified_dataset_name: Optional[str] = None

        self.dataset_identifier: Optional[DatasetIdentifier] = None
        if data_source_impl:
            self.dataset_identifier = DatasetIdentifier(
                data_source_name=self.data_source_impl.name,
                prefixes=self.dataset_prefix,
                dataset_name=self.dataset_name,
            )
            # TODO replace usage of self.sql_qualified_dataset_name with self.dataset_identifier
            self.sql_qualified_dataset_name = data_source_impl.sql_dialect.qualify_dataset_name(
                dataset_prefix=self.dataset_prefix, dataset_name=self.dataset_name
            )

        from soda_core.contracts.impl.check_types.row_count_check import (
            RowCountMetricImpl,
        )

        self.row_count_metric_impl: MetricImpl = self.metrics_resolver.resolve_metric(
            RowCountMetricImpl(contract_impl=self)
        )
        self.dataset_rows_tested: Optional[int] = None

        self.column_impls: list[ColumnImpl] = self._parse_columns(contract_yaml=contract_yaml)
        self.check_impls: list[CheckImpl] = self._parse_checks(contract_yaml)

        dataset_check_impls: list[CheckImpl] = list(self.check_impls)
        column_check_impls: list[CheckImpl] = []
        for column_impl in self.column_impls:
            column_check_impls.extend(column_impl.check_impls)
        # For consistency and predictability, we want the checks eval and results in the same order as in the contract
        self.all_check_impls: list[CheckImpl] = (
            dataset_check_impls + column_check_impls
            if self._dataset_checks_came_before_columns_in_yaml()
            else column_check_impls + dataset_check_impls
        )

        self._verify_duplicate_identities(self.all_check_impls)
        self.metrics: list[MetricImpl] = self.metrics_resolver.get_resolved_metrics()

        self.queries: list[Query] = []
        if data_source_impl:
            self.queries = self._build_queries()

        self.dwh_data_source_file_path: Optional[str] = dwh_data_source_file_path

    def _dataset_checks_came_before_columns_in_yaml(self) -> Optional[bool]:
        contract_keys: list[str] = self.contract_yaml.contract_yaml_object.keys()
        if "checks" in contract_keys and "columns" in contract_keys:
            return contract_keys.index("checks") < contract_keys.index("columns")
        return None

    def _get_data_timestamp(
        self, resolved_variable_values: dict[str, str], soda_variable_values: dict[str, str], default: datetime
    ) -> Optional[datetime]:
        # Skipped for 'NOW' :)
        return None

    def _parse_checks(self, contract_yaml: ContractYaml) -> list[CheckImpl]:
        check_impls: list[CheckImpl] = []
        if contract_yaml.checks:
            for check_yaml in contract_yaml.checks:
                if check_yaml:
                    check = CheckImpl.parse_check(contract_impl=self, check_yaml=check_yaml)
                    check_impls.append(check)
        return check_impls

    def _build_queries(self) -> list[Query]:
        queries: list[Query] = []
        aggregation_metrics: list[AggregationMetricImpl] = []

        for check in self.all_check_impls:
            queries.extend(check.queries)

        for metric in self.metrics:
            if isinstance(metric, AggregationMetricImpl):
                aggregation_metrics.append(metric)

        from soda_core.contracts.impl.check_types.schema_check import SchemaQuery

        schema_queries: list[SchemaQuery] = []
        other_queries: list[SchemaQuery] = []
        for query in queries:
            if isinstance(query, SchemaQuery):
                schema_queries.append(query)
            else:
                other_queries.append(query)

        aggregation_queries: list[AggregationQuery] = []
        for aggregation_metric in aggregation_metrics:
            if len(aggregation_queries) == 0 or not aggregation_queries[-1].can_accept(aggregation_metric):
                aggregation_queries.append(
                    AggregationQuery(
                        dataset_prefix=self.dataset_prefix,
                        dataset_name=self.dataset_name,
                        filter=self.filter,
                        data_source_impl=self.data_source_impl,
                        logs=self.logs,
                    )
                )
            last_aggregation_query: AggregationQuery = aggregation_queries[-1]
            last_aggregation_query.append_aggregation_metric(aggregation_metric)

        return schema_queries + aggregation_queries + other_queries

    def _parse_columns(self, contract_yaml: ContractYaml) -> list[ColumnImpl]:
        columns: list[ColumnImpl] = []
        if contract_yaml.columns:
            for column_yaml in contract_yaml.columns:
                if column_yaml:
                    column = ColumnImpl(contract_impl=self, column_yaml=column_yaml)
                    columns.append(column)
        return columns

    def verify(self) -> ContractVerificationResult:
        data_source: Optional[DataSource] = None
        check_results: list[CheckResult] = []
        measurements: list[Measurement] = []
        contract_verification_status: ContractVerificationStatus = ContractVerificationStatus.UNKNOWN
        dataset_rows_tested: Optional[int] = None

        verb: str = "Validating" if self.only_validate_without_execute else "Verifying"
        logger.info(
            f"{verb} contract {Emoticons.SCROLL} "
            f"{self.contract_yaml.contract_yaml_source.file_path} {Emoticons.FINGERS_CROSSED}"
        )

        if self.data_source_impl:
            data_source = self.data_source_impl.build_data_source()

        if self.logs.has_errors():
            contract_verification_status = ContractVerificationStatus.ERROR

        elif not self.only_validate_without_execute:
            # Executing the queries will set the value of the metrics linked to queries
            for query in self.queries:
                query_measurements: list[Measurement] = query.execute()
                measurements.extend(query_measurements)

            measurement_values: MeasurementValues = MeasurementValues(measurements)

            self.dataset_rows_tested = measurement_values.get_value(self.row_count_metric_impl)

            # Triggering the derived metrics to initialize their value based on their dependencies
            derived_metric_impls: list[DerivedMetricImpl] = [
                derived_metric for derived_metric in self.metrics if isinstance(derived_metric, DerivedMetricImpl)
            ]
            for derived_metric_impl in derived_metric_impls:
                measurement_values.derive_value(derived_metric_impl)

            if self.data_source_impl:
                # Evaluate the checks
                for check_impl in self.all_check_impls:
                    check_result: CheckResult = check_impl.evaluate(measurement_values=measurement_values)
                    check_results.append(check_result)

            contract_verification_status = _get_contract_verification_status(self.logs.records, check_results)

            logger.info(
                self.build_log_summary(
                    soda_qualified_dataset_name=self.soda_qualified_dataset_name, check_results=check_results
                )
            )

        log_records: Optional[list[LogRecord]] = self.logs.pop_log_records()

        soda_cloud_file_id: Optional[str] = None
        sending_results_to_soda_cloud_failed: bool = False
        contract_yaml_source_str_original = self.contract_yaml.contract_yaml_source.yaml_str_original
        soda_cloud_response_json: Optional[dict] = None

        if self.soda_cloud and self.publish_results:
            soda_cloud_file_id = self.soda_cloud._upload_contract_yaml_file(contract_yaml_source_str_original)

        contract_verification_result: ContractVerificationResult = ContractVerificationResult(
            contract=Contract(
                data_source_name=self.data_source_impl.name if self.data_source_impl else None,
                dataset_prefix=self.dataset_prefix,
                dataset_name=self.dataset_name,
                soda_qualified_dataset_name=self.soda_qualified_dataset_name,
                source=YamlFileContentInfo(
                    source_content_str=contract_yaml_source_str_original,
                    local_file_path=self.contract_yaml.contract_yaml_source.file_path,
                    soda_cloud_file_id=soda_cloud_file_id,
                ),
            ),
            data_source=data_source,
            data_timestamp=self.data_timestamp,
            started_timestamp=self.started_timestamp,
            ended_timestamp=datetime.now(tz=timezone.utc),
            measurements=measurements,
            check_results=check_results,
            sending_results_to_soda_cloud_failed=sending_results_to_soda_cloud_failed,
            status=contract_verification_status,
            log_records=log_records,
        )

        if soda_cloud_file_id:
            # send_contract_result will use contract.source.soda_cloud_file_id
            soda_cloud_response_json = self.soda_cloud.send_contract_result(contract_verification_result)
            scan_id: Optional[str] = soda_cloud_response_json.get("scanId") if soda_cloud_response_json else None
            if not scan_id:
                contract_verification_result.sending_results_to_soda_cloud_failed = True
        else:
            logger.debug(f"Not sending results to Soda Cloud {Emoticons.CROSS_MARK}")

        contract_verification_handler: Optional[ContractVerificationHandler] = ContractVerificationHandler.instance()
        if contract_verification_handler:
            contract_verification_handler.handle(
                contract_impl=self,
                data_source_impl=self.data_source_impl,
                contract_verification_result=contract_verification_result,
                soda_cloud=self.soda_cloud,
                soda_cloud_send_results_response_json=soda_cloud_response_json,
                dwh_data_source_file_path=self.dwh_data_source_file_path,
            )

        return contract_verification_result

    def build_log_summary(self, soda_qualified_dataset_name: str, check_results: list[CheckResult]) -> str:
        summary_lines: list[str] = []

        failed_count: int = 0
        not_evaluated_count: int = 0
        passed_count: int = 0

        for check_result in check_results:
            if check_result.is_failed:
                failed_count += 1
            elif check_result.is_not_evaluated:
                not_evaluated_count += 1
            elif check_result.is_passed:
                passed_count += 1
        total_count: int = failed_count + not_evaluated_count + passed_count

        error_count: int = len(self.logs.get_errors())

        table_lines = [
            ["Checks", total_count],
            ["Passed", passed_count, Emoticons.WHITE_CHECK_MARK],
        ]

        if failed_count > 0:
            table_lines.append(["Failed", failed_count, Emoticons.CROSS_MARK])
        else:
            table_lines.append(["Failed", failed_count, Emoticons.WHITE_CHECK_MARK])

        if not_evaluated_count > 0:
            table_lines.append(["Not Evaluated", not_evaluated_count, Emoticons.CROSS_MARK])
        else:
            table_lines.append(["Not Evaluated", not_evaluated_count, Emoticons.WHITE_CHECK_MARK])
        if error_count > 0:
            table_lines.append(["Runtime Errors", error_count, Emoticons.CROSS_MARK])
        else:
            table_lines.append(["Runtime Errors", error_count, Emoticons.WHITE_CHECK_MARK])

        summary_lines.append(f"\n### Contract results for {soda_qualified_dataset_name}")
        summary_lines.append(self.build_summary_table(check_results))

        overview_table = tabulate(table_lines, tablefmt="github", stralign="left")
        summary_lines.append(f"# Summary:\n{overview_table}\n")

        return "\n".join(summary_lines)

    def build_summary_table(self, check_results: list[CheckResult]) -> str:
        overview_table_data = [check_result.log_table_row() for check_result in check_results]

        # Sort by column name, check name and check outcome
        overview_table_data.sort(key=lambda row: (row["Column"], row["Check"], row["Outcome"]))

        # Re-iterate rows data and remove column name if it is the same as the previous row
        previous_column_name: Optional[str] = None
        for row in overview_table_data:
            if previous_column_name == row["Column"]:
                row["Column"] = ""  # Clear column name if it is the same as the previous row
            else:
                previous_column_name = row["Column"]

        return tabulate(overview_table_data, headers="keys", tablefmt="grid")

    @classmethod
    def _verify_duplicate_identities(cls, all_check_impls: list[CheckImpl]):
        checks_by_identity: dict[str, CheckImpl] = {}
        for check_impl in all_check_impls:
            existing_check_impl: Optional[CheckImpl] = checks_by_identity.get(check_impl.identity)
            if existing_check_impl:
                original_location: Optional[Location] = existing_check_impl.check_yaml.check_yaml_object.location
                original_location_str: str = f" Original({original_location})" if original_location else ""
                duplicate_location: Optional[Location] = check_impl.check_yaml.check_yaml_object.location
                duplicate_location_str: str = f" Duplicate({duplicate_location})" if duplicate_location else ""
                logger.error(
                    msg=(
                        f"Duplicate identity {check_impl.build_identity_path()}."
                        f"{original_location_str}{duplicate_location_str}"
                    ),
                    extra={
                        ExtraKeys.LOCATION: duplicate_location,
                    },
                )
            checks_by_identity[check_impl.identity] = check_impl

    @classmethod
    def compute_data_quality_score(cls, total_failed_rows_count: int, total_rows_count: int) -> float:
        return 100 - (total_failed_rows_count * 100 / total_rows_count)


def _get_contract_verification_status(
    log_records: list[logging.LogRecord], check_results: list[CheckResult]
) -> ContractVerificationStatus:
    if any(r.levelno >= logging.ERROR for r in log_records):
        return ContractVerificationStatus.ERROR

    if any(check_result.outcome == CheckOutcome.FAILED for check_result in check_results):
        return ContractVerificationStatus.FAILED

    if all(check_result.outcome == CheckOutcome.PASSED for check_result in check_results):
        return ContractVerificationStatus.PASSED

    return ContractVerificationStatus.UNKNOWN


class MeasurementValues:
    def __init__(self, measurements: list[Measurement]):
        self.metric_values_by_id: dict[str, any] = {
            measurement.metric_id: measurement.value for measurement in measurements
        }
        self.metric_ids_being_derived: set[str] = set()

    def get_value(self, metric_impl: MetricImpl) -> any:
        return self.metric_values_by_id.get(metric_impl.id)

    def derive_value(self, derived_metric_impl: DerivedMetricImpl) -> None:
        if derived_metric_impl.id not in self.metric_values_by_id:
            if derived_metric_impl.id in self.metric_ids_being_derived:
                logger.error("Bug: please report circular reference in derived metrics")
            else:
                self.metric_ids_being_derived.add(derived_metric_impl.id)
                for metric_dependency in derived_metric_impl.get_metric_dependencies():
                    if isinstance(metric_dependency, DerivedMetricImpl):
                        self.derive_value(metric_dependency)
                self.metric_ids_being_derived.remove(derived_metric_impl.id)
                value = derived_metric_impl.compute_derived_value(self)
                self.metric_values_by_id[derived_metric_impl.id] = value


class ColumnImpl:
    def __init__(self, contract_impl: ContractImpl, column_yaml: ColumnYaml):
        self.column_yaml = column_yaml
        self.missing_and_validity: MissingAndValidity = MissingAndValidity(missing_and_validity_yaml=column_yaml)
        self.check_impls: list[CheckImpl] = []
        if column_yaml.check_yamls:
            for check_yaml in column_yaml.check_yamls:
                if check_yaml:
                    check = CheckImpl.parse_check(
                        contract_impl=contract_impl,
                        column_impl=self,
                        check_yaml=check_yaml,
                    )
                    self.check_impls.append(check)


class ValidReferenceData:
    @classmethod
    def parse(cls, valid_reference_data_yaml: Optional[ValidReferenceDataYaml]) -> Optional[ValidReferenceData]:
        return (
            ValidReferenceData(valid_reference_data_yaml)
            if valid_reference_data_yaml and valid_reference_data_yaml.dataset and valid_reference_data_yaml.column
            else None
        )

    def __init__(self, valid_reference_data_yaml: ValidReferenceDataYaml):
        dataset_identifier: DatasetIdentifier = DatasetIdentifier.parse(valid_reference_data_yaml.dataset)
        self.data_source_name: str = dataset_identifier.data_source_name
        self.dataset_prefix: list[str] = dataset_identifier.prefixes
        self.dataset_name: str = dataset_identifier.dataset_name
        self.column: str = valid_reference_data_yaml.column


class MissingAndValidity:
    def __init__(self, missing_and_validity_yaml: MissingAndValidityYaml):
        self.missing_values: Optional[list] = missing_and_validity_yaml.missing_values
        self.missing_format: Optional[RegexFormat] = missing_and_validity_yaml.missing_format

        self.invalid_values: Optional[list] = missing_and_validity_yaml.invalid_values
        self.invalid_format: Optional[RegexFormat] = missing_and_validity_yaml.invalid_format
        self.valid_values: Optional[list] = missing_and_validity_yaml.valid_values
        self.valid_format: Optional[RegexFormat] = missing_and_validity_yaml.valid_format
        self.valid_min: Optional[Number] = missing_and_validity_yaml.valid_min
        self.valid_max: Optional[Number] = missing_and_validity_yaml.valid_max
        self.valid_length: Optional[int] = missing_and_validity_yaml.valid_length
        self.valid_min_length: Optional[int] = missing_and_validity_yaml.valid_min_length
        self.valid_max_length: Optional[int] = missing_and_validity_yaml.valid_max_length
        self.valid_reference_data: Optional[ValidReferenceData] = ValidReferenceData.parse(
            missing_and_validity_yaml.valid_reference_data
        )

    def is_missing_expr(self, column_name: str | COLUMN) -> SqlExpression:
        is_missing_clauses: list[SqlExpression] = [IS_NULL(column_name)]
        if isinstance(self.missing_values, list):
            literal_values = [LITERAL(value) for value in self.missing_values]
            is_missing_clauses.append(IN(column_name, literal_values))
        if isinstance(self.missing_format, RegexFormat) and isinstance(self.missing_format.regex, str):
            is_missing_clauses.append(REGEX_LIKE(column_name, self.missing_format.regex))
        return OR.optional(is_missing_clauses)

    def is_invalid_expr(self, column_name: str | COLUMN) -> Optional[SqlExpression]:
        invalid_clauses: list[SqlExpression] = []
        if isinstance(self.valid_values, list):
            literal_values = [LITERAL(value) for value in self.valid_values if value is not None]
            if None in self.valid_values:
                invalid_clauses.append(AND([NOT(IN(column_name, literal_values)), IS_NOT_NULL(column_name)]))
            elif not self.valid_values:
                invalid_clauses.append(AND([LITERAL(True)]))
            else:
                invalid_clauses.append(NOT(IN(column_name, literal_values)))
        if isinstance(self.invalid_values, list):
            literal_values = [LITERAL(value) for value in self.invalid_values if value is not None]
            if None in self.invalid_values:
                invalid_clauses.append(AND([IN(column_name, literal_values), IS_NULL(column_name)]))
            elif not self.invalid_values:
                invalid_clauses.append(AND([LITERAL(False)]))
            else:
                invalid_clauses.append(IN(column_name, literal_values))
        if isinstance(self.valid_format, RegexFormat) and isinstance(self.valid_format.regex, str):
            invalid_clauses.append(NOT(REGEX_LIKE(column_name, self.valid_format.regex)))
        if isinstance(self.valid_min, Number) or isinstance(self.valid_min, str):
            invalid_clauses.append(LT(column_name, LITERAL(self.valid_min)))
        if isinstance(self.valid_max, Number) or isinstance(self.valid_max, str):
            invalid_clauses.append(GT(column_name, LITERAL(self.valid_max)))
        if isinstance(self.invalid_format, RegexFormat) and isinstance(self.invalid_format.regex, str):
            invalid_clauses.append(REGEX_LIKE(column_name, self.invalid_format.regex))
        if isinstance(self.valid_length, int):
            invalid_clauses.append(NEQ(LENGTH(column_name), LITERAL(self.valid_length)))
        if isinstance(self.valid_min_length, int):
            invalid_clauses.append(LT(LENGTH(column_name), LITERAL(self.valid_min_length)))
        if isinstance(self.valid_max_length, int):
            invalid_clauses.append(GT(LENGTH(column_name), LITERAL(self.valid_max_length)))
        return OR.optional(invalid_clauses)

    def is_valid_expr(self, column_name: str | COLUMN) -> SqlExpression:
        return NOT.optional(OR.optional([self.is_missing_expr(column_name), self.is_invalid_expr(column_name)]))

    @classmethod
    def __apply_default(cls, self_value, default_value) -> any:
        if self_value is not None:
            return self_value
        return default_value

    def apply_column_defaults(self, column_impl: ColumnImpl) -> None:
        if not column_impl:
            return
        column_defaults: MissingAndValidity = column_impl.missing_and_validity
        if not column_defaults:
            return

        check_has_missing: bool = self.has_missing_configurations()
        self.missing_values = self.missing_values if check_has_missing else column_defaults.missing_values
        self.missing_format = self.missing_format if check_has_missing else column_defaults.missing_format

        check_has_validity: bool = self.has_validity_configurations()
        self.invalid_values = self.invalid_values if check_has_validity else column_defaults.invalid_values
        self.invalid_format = self.invalid_format if check_has_validity else column_defaults.invalid_format
        self.valid_values = self.valid_values if check_has_validity else column_defaults.valid_values
        self.valid_format = self.valid_format if check_has_validity else column_defaults.valid_format
        self.valid_min = self.valid_min if check_has_validity else column_defaults.valid_min
        self.valid_max = self.valid_max if check_has_validity else column_defaults.valid_max
        self.valid_length = self.valid_length if check_has_validity else column_defaults.valid_length
        self.valid_min_length = self.valid_min_length if check_has_validity else column_defaults.valid_min_length
        self.valid_max_length = self.valid_max_length if check_has_validity else column_defaults.valid_max_length
        self.valid_reference_data = (
            self.valid_reference_data if check_has_validity else column_defaults.valid_reference_data
        )

    def has_missing_configurations(self) -> bool:
        return self.missing_values is not None or self.missing_format is not None

    def has_validity_configurations(self) -> bool:
        return any(
            cfg is not None
            for cfg in [
                self.invalid_values,
                self.invalid_format,
                self.valid_values,
                self.valid_format,
                self.valid_min,
                self.valid_max,
                self.valid_length,
                self.valid_min_length,
                self.valid_max_length,
                self.valid_reference_data,
            ]
        )

    def has_reference_data(self) -> bool:
        return isinstance(self.valid_reference_data, ValidReferenceData)


class MetricsResolver:
    def __init__(self):
        self.metrics: list[MetricImpl] = []

    def resolve_metric(self, metric_impl: MetricImpl) -> MetricImpl:
        existing_metric_impl: Optional[MetricImpl] = next((m for m in self.metrics if m == metric_impl), None)
        if existing_metric_impl:
            return existing_metric_impl
        else:
            self.metrics.append(metric_impl)
            return metric_impl

    def get_resolved_metrics(self) -> list[MetricImpl]:
        return self.metrics


class ThresholdType(Enum):
    SINGLE_COMPARATOR = "single_comparator"
    INNER_RANGE = "inner_range"
    OUTER_RANGE = "outer_range"


class ThresholdImpl:
    @classmethod
    def create(
        cls, threshold_yaml: ThresholdYaml, default_threshold: Optional[ThresholdImpl] = None
    ) -> Optional[ThresholdImpl]:
        if threshold_yaml is None:
            if default_threshold:
                return default_threshold
            else:
                logger.error(f"Threshold required, but not specified")
                return None

        if not threshold_yaml.has_any_configurations():
            if default_threshold:
                return default_threshold
            logger.error(f"Threshold required, but not specified")
            return None

        if threshold_yaml.has_exactly_one_comparison():
            return ThresholdImpl(
                type=ThresholdType.SINGLE_COMPARATOR,
                must_be_greater_than=threshold_yaml.must_be_greater_than,
                must_be_greater_than_or_equal=threshold_yaml.must_be_greater_than_or_equal,
                must_be_less_than=threshold_yaml.must_be_less_than,
                must_be_less_than_or_equal=threshold_yaml.must_be_less_than_or_equal,
                must_be=threshold_yaml.must_be,
                must_not_be=threshold_yaml.must_not_be,
            )

        elif threshold_yaml.must_be_between:
            range_error: Optional[str] = threshold_yaml.must_be_between.get_between_range_error()
            if range_error:
                logger.error(f"Invalid between threshold range: {range_error}")
                return None
            else:
                return ThresholdImpl(
                    type=ThresholdType.INNER_RANGE,
                    must_be_greater_than=threshold_yaml.must_be_between.greater_than,
                    must_be_greater_than_or_equal=threshold_yaml.must_be_between.greater_than_or_equal,
                    must_be_less_than=threshold_yaml.must_be_between.less_than,
                    must_be_less_than_or_equal=threshold_yaml.must_be_between.less_than_or_equal,
                )

        elif threshold_yaml.must_be_not_between:
            range_error: Optional[str] = threshold_yaml.must_be_not_between.get_not_between_range_error()
            if range_error:
                logger.error(f"Invalid not between threshold range: {range_error}")
                return None
            else:
                return ThresholdImpl(
                    type=ThresholdType.OUTER_RANGE,
                    must_be_greater_than=threshold_yaml.must_be_not_between.greater_than,
                    must_be_greater_than_or_equal=threshold_yaml.must_be_not_between.greater_than_or_equal,
                    must_be_less_than=threshold_yaml.must_be_not_between.less_than,
                    must_be_less_than_or_equal=threshold_yaml.must_be_not_between.less_than_or_equal,
                )

    def __init__(
        self,
        type: ThresholdType,
        must_be_greater_than: Optional[Number] = None,
        must_be_greater_than_or_equal: Optional[Number] = None,
        must_be_less_than: Optional[Number] = None,
        must_be_less_than_or_equal: Optional[Number] = None,
        must_be: Optional[Number] = None,
        must_not_be: Optional[Number] = None,
    ):
        self.type: ThresholdType = type
        self.must_be_greater_than: Optional[Number] = must_be_greater_than
        self.must_be_greater_than_or_equal: Optional[Number] = must_be_greater_than_or_equal
        self.must_be_less_than: Optional[Number] = must_be_less_than
        self.must_be_less_than_or_equal: Optional[Number] = must_be_less_than_or_equal
        self.must_be: Optional[Number] = must_be
        self.must_not_be: Optional[Number] = must_not_be

    def to_threshold_info(self) -> Threshold:
        if self.must_be is None and self.must_not_be is None:
            return Threshold(
                must_be_greater_than=self.must_be_greater_than,
                must_be_greater_than_or_equal=self.must_be_greater_than_or_equal,
                must_be_less_than=self.must_be_less_than,
                must_be_less_than_or_equal=self.must_be_less_than_or_equal,
            )
        elif self.must_be is not None:
            return Threshold(must_be_greater_than_or_equal=self.must_be, must_be_less_than_or_equal=self.must_be)
        elif self.must_not_be is not None:
            return Threshold(must_be_greater_than=self.must_not_be, must_be_less_than=self.must_not_be)

    @classmethod
    def get_metric_name(cls, metric_name: str, column_impl: Optional[ColumnImpl]) -> str:
        if column_impl:
            return f"{metric_name}({column_impl.column_yaml.name})"
        else:
            return metric_name

    def get_assertion_summary(self, metric_name: str) -> str:
        """
        For ease of reading, thresholds always list small values lef and big values right (where applicable).
        Eg '0 < metric_name', 'metric_name < 25', '0 <= metric_name < 25'
        """
        if self.type == ThresholdType.SINGLE_COMPARATOR:
            if isinstance(self.must_be_greater_than, Number):
                return f"{self.must_be_greater_than} < {metric_name}"
            if isinstance(self.must_be_greater_than_or_equal, Number):
                return f"{self.must_be_greater_than_or_equal} <= {metric_name}"
            if isinstance(self.must_be_less_than, Number):
                return f"{metric_name} < {self.must_be_less_than}"
            if isinstance(self.must_be_less_than_or_equal, Number):
                return f"{metric_name} <= {self.must_be_less_than_or_equal}"
            if isinstance(self.must_be, Number):
                return f"{metric_name} = {self.must_be}"
            if isinstance(self.must_not_be, Number):
                return f"{metric_name} != {self.must_not_be}"
        elif self.type == ThresholdType.INNER_RANGE or self.type == ThresholdType.OUTER_RANGE:
            gt_comparator: str = " < " if isinstance(self.must_be_greater_than, Number) else " <= "
            gt_bound: str = (
                str(self.must_be_greater_than)
                if isinstance(self.must_be_greater_than, Number)
                else str(self.must_be_greater_than_or_equal)
            )
            lt_comparator: str = " < " if isinstance(self.must_be_less_than, Number) else " <= "
            lt_bound: str = (
                str(self.must_be_less_than)
                if isinstance(self.must_be_less_than, Number)
                else str(self.must_be_less_than_or_equal)
            )
            if self.type == ThresholdType.INNER_RANGE:
                return f"{gt_bound}{gt_comparator}{metric_name}{lt_comparator}{lt_bound}"
            else:
                return f"{metric_name}{lt_comparator}{lt_bound} or {gt_bound}{gt_comparator}{metric_name}"

    def passes(self, value: Number) -> bool:
        is_greater_than_ok: bool = (self.must_be_greater_than is None or value > self.must_be_greater_than) and (
            self.must_be_greater_than_or_equal is None or value >= self.must_be_greater_than_or_equal
        )
        is_less_than_ok: bool = (self.must_be_less_than is None or value < self.must_be_less_than) and (
            self.must_be_less_than_or_equal is None or value <= self.must_be_less_than_or_equal
        )
        if self.type == ThresholdType.OUTER_RANGE:
            return is_greater_than_ok or is_less_than_ok
        else:
            return (
                is_greater_than_ok
                and is_less_than_ok
                and (not (isinstance(self.must_be, Number) or isinstance(self.must_be, str)) or value == self.must_be)
                and (
                    not (isinstance(self.must_not_be, Number) or isinstance(self.must_not_be, str))
                    or value != self.must_not_be
                )
            )


class CheckParser(ABC):
    @abstractmethod
    def get_check_type_names(self) -> list[str]:
        pass

    @abstractmethod
    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: CheckYaml,
    ) -> Optional[CheckImpl]:
        pass


class CheckImpl:
    check_parsers: dict[str, CheckParser] = {}

    @classmethod
    def register(cls, check_parser: CheckParser) -> None:
        for check_type_name in check_parser.get_check_type_names():
            cls.check_parsers[check_type_name] = check_parser

    @classmethod
    def get_check_type_names(cls) -> list[str]:
        return list(cls.check_parsers.keys())

    @classmethod
    def parse_check(
        cls,
        contract_impl: ContractImpl,
        check_yaml: CheckYaml,
        column_impl: Optional[ColumnImpl] = None,
    ) -> Optional[CheckImpl]:
        if isinstance(check_yaml.type_name, str):
            check_parser: Optional[CheckParser] = cls.check_parsers.get(check_yaml.type_name)
            if check_parser:
                return check_parser.parse_check(
                    contract_impl=contract_impl,
                    column_impl=column_impl,
                    check_yaml=check_yaml,
                )
            else:
                logger.error(f"Unknown check type '{check_yaml.type_name}'")

    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: CheckYaml,
    ):
        self.logs: Logs = contract_impl.logs

        self.contract_impl: ContractImpl = contract_impl
        self.check_yaml: CheckYaml = check_yaml
        self.name: str = self._get_name_with_default(check_yaml)
        self.column_impl: Optional[ColumnImpl] = column_impl
        self.type: str = check_yaml.type_name
        self.identity: str = self._build_identity(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_type=check_yaml.type_name,
            qualifier=check_yaml.qualifier,
        )

        self.threshold: Optional[ThresholdImpl] = None
        self.metrics: list[MetricImpl] = []
        self.queries: list[Query] = []
        self.skip: bool = False

        # Merge check attributes with contract attributes
        self.attributes: dict[str, any] = {**contract_impl.check_attributes, **check_yaml.attributes}

    __DEFAULT_CHECK_NAMES_BY_TYPE: dict[str, str] = {
        "schema": "Schema matches expected structure",
        "row_count": "Row count meets expected threshold",
        "freshness": "Data is fresh",
        "missing": "No missing values",
        "invalid": "No invalid values",
        "duplicate": "No duplicate values",
        "aggregate": "Metric function meets threshold",
        "metric": "Metric meets threshold",
        "failed_rows": "No rows violating the condition",
    }

    def _get_name_with_default(self, check_yaml: CheckYaml) -> str:
        if isinstance(check_yaml.name, str):
            return check_yaml.name
        default_check_name: Optional[str] = self.__DEFAULT_CHECK_NAMES_BY_TYPE.get(check_yaml.type_name)
        if isinstance(default_check_name, str):
            return default_check_name
        return check_yaml.type_name

    def _resolve_metric(self, metric_impl: MetricImpl) -> MetricImpl:
        resolved_metric_impl: MetricImpl = self.contract_impl.metrics_resolver.resolve_metric(metric_impl)
        self.metrics.append(resolved_metric_impl)
        return resolved_metric_impl

    @abstractmethod
    def evaluate(self, measurement_values: MeasurementValues) -> CheckResult:
        pass

    def _build_check_info(self) -> Check:
        return Check(
            type=self.type,
            qualifier=self.check_yaml.qualifier,
            name=self.name,
            identity=self.identity,
            definition=self._build_definition(),
            column_name=self.column_impl.column_yaml.name if self.column_impl else None,
            contract_file_line=self.check_yaml.check_yaml_object.location.line,
            contract_file_column=self.check_yaml.check_yaml_object.location.column,
            threshold=self._build_threshold(),
            attributes=self.attributes,
            location=self.check_yaml.check_yaml_object.location,
        )

    @classmethod
    def _build_identity(
        cls, contract_impl: ContractImpl, column_impl: Optional[ColumnImpl], check_type: str, qualifier: Optional[str]
    ) -> str:
        identity_hash_builder: ConsistentHashBuilder = ConsistentHashBuilder(8)
        if contract_impl.data_source_impl:
            identity_hash_builder.add_property("dso", contract_impl.data_source_impl.name)
        identity_hash_builder.add_property("pr", contract_impl.dataset_prefix)
        identity_hash_builder.add_property("ds", contract_impl.dataset_name)
        identity_hash_builder.add_property("c", column_impl.column_yaml.name if column_impl else None)
        identity_hash_builder.add_property("t", check_type)
        identity_hash_builder.add_property("q", qualifier)

        return identity_hash_builder.get_hash()

    def build_identity_path(self) -> str:
        parts: list[Optional[str]] = [
            self.contract_impl.contract_yaml.contract_yaml_source.file_path,
            self.column_impl.column_yaml.name if self.column_impl else None,
            self.type,
            self.check_yaml.qualifier if self.check_yaml else None,
        ]
        parts = [p for p in parts if p is not None]
        return "/".join(parts)

    def _build_definition(self) -> str:
        contract_dict: dict = {}
        if self.contract_impl.contract_yaml.filter:
            contract_dict["filter"] = self.contract_impl.contract_yaml.filter

        check_dict: dict = self.check_yaml.check_yaml_object.yaml_dict

        if self.column_impl:
            contract_dict["columns"] = [{"name": self.column_impl.column_yaml.name, "checks": [check_dict]}]
        else:
            contract_dict["checks"] = [check_dict]

        text_stream = StringIO()
        yaml = YAML()
        yaml.dump(contract_dict, text_stream)
        text_stream.seek(0)
        return text_stream.read()

    def _build_threshold(self) -> Optional[Threshold]:
        return self.threshold.to_threshold_info() if self.threshold else None

    def get_threshold_metric_impl(self) -> Optional[MetricImpl]:
        """
        Used in extensions
        """
        raise SodaException(f"Check type '{self.type}' does not support get_threshold_metric_impl'")


class MissingAndValidityCheckImpl(CheckImpl):
    def __init__(
        self, contract_impl: ContractImpl, column_impl: Optional[ColumnImpl], check_yaml: MissingAncValidityCheckYaml
    ):
        super().__init__(contract_impl, column_impl, check_yaml)
        self.missing_and_validity: MissingAndValidity = MissingAndValidity(missing_and_validity_yaml=check_yaml)
        self.missing_and_validity.apply_column_defaults(column_impl)


class MetricImpl:
    def __init__(
        self,
        contract_impl: ContractImpl,
        metric_type: str,
        column_impl: Optional[ColumnImpl] = None,
        check_filter: Optional[str] = None,
        missing_and_validity: Optional[MissingAndValidity] = None,
    ):
        self.contract_impl: ContractImpl = contract_impl
        self.column_impl: Optional[ColumnImpl] = column_impl
        self.type: str = metric_type
        self.check_filter: Optional[str] = check_filter
        self.missing_and_validity: Optional[MissingAndValidity] = missing_and_validity
        self.id: str = self._build_id()

    def _build_id(self) -> str:
        hash_builder: ConsistentHashBuilder = ConsistentHashBuilder(hash_string_length=8)
        id_properties: dict[str, any] = self._get_id_properties()
        for k, v in id_properties.items():
            hash_builder.add_property(k, v)
        return hash_builder.get_hash()

    def _get_id_properties(self) -> dict[str, any]:
        id_properties: dict[str, any] = {"type": self.type}

        if self.contract_impl and self.contract_impl.contract_yaml:
            id_properties["dataset"] = self.contract_impl.contract_yaml.dataset
        if self.column_impl:
            id_properties["column"] = self.column_impl.column_yaml.name
        if self.check_filter:
            id_properties["check_filter"] = self.check_filter
        if self.missing_and_validity:
            id_properties["missing_values"] = self.missing_and_validity.missing_values
            id_properties["missing_format"] = self.missing_and_validity.missing_format
            id_properties["invalid_values"] = self.missing_and_validity.invalid_values
            if self.missing_and_validity.invalid_format:
                id_properties["invalid_format_regex"] = self.missing_and_validity.invalid_format.regex
            id_properties["valid_values"] = self.missing_and_validity.valid_values
            if self.missing_and_validity.valid_format:
                id_properties["valid_format"] = self.missing_and_validity.valid_format.regex
            id_properties["valid_min"] = self.missing_and_validity.valid_min
            id_properties["valid_max"] = self.missing_and_validity.valid_max
            id_properties["valid_length"] = self.missing_and_validity.valid_length
            id_properties["valid_min_length"] = self.missing_and_validity.valid_min_length
            id_properties["valid_max_length"] = self.missing_and_validity.valid_max_length
            if self.missing_and_validity.valid_reference_data:
                id_properties["ref_prefix"] = self.missing_and_validity.valid_reference_data.dataset_prefix
                id_properties["ref_name"] = self.missing_and_validity.valid_reference_data.dataset_name
                id_properties["ref_column"] = self.missing_and_validity.valid_reference_data.column

        return id_properties

    def __eq__(self, other):
        if type(other) != type(self):
            return False
        return self.id == other.id

    @abstractmethod
    def sql_condition_expression(self) -> Optional[SqlExpression]:
        pass


class AggregationMetricImpl(MetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        metric_type: str,
        column_impl: Optional[ColumnImpl] = None,
        check_filter: Optional[str] = None,
        missing_and_validity: Optional[MissingAndValidity] = None,
    ):
        super().__init__(
            contract_impl=contract_impl,
            metric_type=metric_type,
            column_impl=column_impl,
            check_filter=check_filter,
            missing_and_validity=missing_and_validity,
        )

    @abstractmethod
    def sql_expression(self) -> SqlExpression:
        pass

    @abstractmethod
    def sql_condition_expression(self) -> SqlExpression:
        """
        Used in extensions
        """

    def convert_db_value(self, value: any) -> any:
        return value

    def get_short_description(self) -> str:
        return self.type


class DerivedMetricImpl(MetricImpl, ABC):
    @abstractmethod
    def get_metric_dependencies(self) -> list[MetricImpl]:
        pass

    @abstractmethod
    def compute_derived_value(self, measurement_values: MeasurementValues) -> Number:
        pass

    def _get_id_properties(self) -> dict[str, any]:
        id_properties: dict[str, any] = super()._get_id_properties()
        for index, metric_dependency in enumerate(self.get_metric_dependencies()):
            id_properties[str(index)] = metric_dependency.id
        return id_properties


class DerivedPercentageMetricImpl(DerivedMetricImpl):
    def __init__(self, metric_type: str, fraction_metric_impl: MetricImpl, total_metric_impl: MetricImpl):
        self.fraction_metric_impl: MetricImpl = fraction_metric_impl
        self.total_metric_impl: MetricImpl = total_metric_impl
        # Mind the ordering as the self._build_id() must come last
        super().__init__(
            contract_impl=fraction_metric_impl.contract_impl,
            column_impl=fraction_metric_impl.column_impl,
            metric_type=metric_type,
            check_filter=None,
        )

    def get_metric_dependencies(self) -> list[MetricImpl]:
        return [self.fraction_metric_impl, self.total_metric_impl]

    def compute_derived_value(self, measurement_values: MeasurementValues) -> Number:
        fraction: int = measurement_values.get_value(self.fraction_metric_impl)
        if fraction is None:
            return 0
        total: int = measurement_values.get_value(self.total_metric_impl)
        return (fraction * 100 / total) if total != 0 else 0


class ValidCountMetric(AggregationMetricImpl):
    def __init__(self, contract_impl: ContractImpl, column_impl: ColumnImpl, check_impl: MissingAndValidityCheckImpl):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            metric_type="valid_count",
            check_filter=check_impl.check_yaml.filter,
            missing_and_validity=check_impl.missing_and_validity,
        )

    def sql_expression(self) -> SqlExpression:
        column_name = self.column_impl.column_yaml.name
        filters: list = [SqlExpressionStr.optional(self.check_filter)]
        if self.missing_and_validity:
            filters.append(NOT(self.missing_and_validity.is_missing_expr(column_name)))
            filters.append(NOT.optional(self.missing_and_validity.is_invalid_expr(column_name)))
        if filters:
            return SUM(CASE_WHEN(AND.optional(filters), LITERAL(1)))
        else:
            return COUNT(column_name)

    def convert_db_value(self, value) -> int:
        # Note: expression SUM(CASE WHEN "id" IS NULL THEN 1 ELSE 0 END) gives NULL / None as a result if
        # there are no rows
        return int(value) if value is not None else 0


class Query(ABC):
    def __init__(
        self, data_source_impl: Optional[DataSourceImpl], metrics: list[MetricImpl], sql: Optional[str] = None
    ):
        self.data_source_impl: DataSourceImpl = data_source_impl
        self.metrics: list[MetricImpl] = metrics
        self.sql: Optional[str] = sql

    def build_sql(self) -> str:
        """
        Signals the query building process is done.  The framework assumes that after this call the self.sql is initialized with the query string.
        """
        return self.sql

    @abstractmethod
    def execute(self) -> list[Measurement]:
        pass


class AggregationQuery(Query):
    def __init__(
        self,
        dataset_prefix: list[str],
        dataset_name: str,
        filter: Optional[str],
        data_source_impl: Optional[DataSourceImpl],
        logs: Logs,
    ):
        super().__init__(data_source_impl=data_source_impl, metrics=[])
        self.dataset_prefix: list[str] = dataset_prefix
        self.dataset_name: str = dataset_name
        self.filter: str = filter
        self.aggregation_metrics: list[AggregationMetricImpl] = []
        self.data_source_impl: DataSourceImpl = data_source_impl
        self.query_size: int = len(self.build_sql())
        self.logs: Logs = logs

    def can_accept(self, aggregation_metric_impl: AggregationMetricImpl) -> bool:
        sql_expression: SqlExpression = aggregation_metric_impl.sql_expression()
        sql_expression_str: str = self.data_source_impl.sql_dialect.build_expression_sql(sql_expression)
        return self.query_size + len(sql_expression_str) < self.data_source_impl.get_max_aggregation_query_length()

    def append_aggregation_metric(self, aggregation_metric_impl: AggregationMetricImpl) -> None:
        self.aggregation_metrics.append(aggregation_metric_impl)

    def build_sql(self) -> str:
        field_expressions: list[SqlExpression] = self.build_field_expressions()
        select = [SELECT(field_expressions), FROM(self.dataset_name, self.dataset_prefix)]
        if self.filter:
            select.append(WHERE(SqlExpressionStr(self.filter)))
        self.sql = self.data_source_impl.sql_dialect.build_select_sql(select)
        return self.sql

    def build_field_expressions(self) -> list[SqlExpression]:
        if len(self.aggregation_metrics) == 0:
            # This is to get the initial query length in the constructor
            return [COUNT(STAR())]
        return [aggregation_metric.sql_expression() for aggregation_metric in self.aggregation_metrics]

    def execute(self) -> list[Measurement]:
        sql = self.build_sql()
        try:
            query_result: QueryResult = self.data_source_impl.execute_query(sql)
        except Exception as e:
            if invalid_regex_exc := InvalidRegexException.should_raise(e, sql):
                raise invalid_regex_exc
            raise SodaCoreException(f"Could not execute aggregation query: {e}") from e

        measurements: list[Measurement] = []
        row: tuple = query_result.rows[0]
        for i in range(0, len(self.aggregation_metrics)):
            aggregation_metric_impl: AggregationMetricImpl = self.aggregation_metrics[i]
            measurement_value = aggregation_metric_impl.convert_db_value(row[i])
            measurements.append(
                Measurement(
                    metric_id=aggregation_metric_impl.id,
                    value=measurement_value,
                    metric_name=aggregation_metric_impl.get_short_description(),
                )
            )
        return measurements
