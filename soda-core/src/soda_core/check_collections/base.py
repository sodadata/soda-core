"""Base classes for check-collection verification.

A check collection is one verifiable YAML file (a contract, a data standard,
...). ``CheckCollectionImpl`` is the engine. Subclasses declare four plain
class attributes:

    wire_source:  str  — Cloud upload ``"source"`` literal (e.g. ``"soda-contract"``).
    display_name: str  — User-facing word in logs and errors.
    yaml_class:   type[CheckCollectionYaml]  — YAML parser used by the executor.
    result_class: type[CheckCollectionResult] — Concrete result returned by ``verify()``.

Everything else (parse columns, parse checks, resolve metrics, build queries,
execute, upload to Soda Cloud, run post-processing handlers) is inherited.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from logging import LogRecord
from numbers import Number
from typing import Any, Optional

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.exceptions import SodaCoreException, get_exception_stacktrace
from soda_core.common.logging_constants import Emoticons, ExtraKeys, soda_logger
from soda_core.common.logs import Location, Logs
from soda_core.common.metadata_types import SamplerType
from soda_core.common.soda_cloud_converter import map_sampler_type_from_dto
from soda_core.common.soda_cloud_dto import DatasetConfigurationDTO
from soda_core.common.sql_dialect import (
    CTE,
    FROM,
    SELECT,
    STAR,
    WHERE,
    DatasetIdentifier,
    SqlExpressionStr,
)
from soda_core.common.yaml import CheckCollectionYamlSource
from soda_core.contracts.contract_verification import (
    CheckCollectionResult,
    CheckOutcome,
    CheckResult,
    Contract,
    ContractVerificationStatus,
    DataSource,
    Measurement,
    PostProcessingStage,
    YamlFileContentInfo,
)

logger: logging.Logger = soda_logger


class CheckCollectionYaml:
    """Parsed YAML for one check-collection file.

    Concrete subtypes (``ContractYaml``, ``DataStandardYaml``, ...) inherit
    from this. They are responsible for their own ``parse()`` classmethod;
    the executor calls ``cls.yaml_class.parse(...)``.
    """

    @classmethod
    def parse(
        cls,
        yaml_source: CheckCollectionYamlSource,
        provided_variable_values: Optional[dict[str, str]] = None,
        data_timestamp: Optional[str] = None,
        primary_data_source_impl: Optional[DataSourceImpl] = None,
    ) -> "CheckCollectionYaml":
        """Subclasses override with their own parse logic."""
        raise NotImplementedError(f"{cls.__name__} does not implement parse(...)")


class CheckCollectionSessionResult:
    """Result of verifying multiple check-collection files in one session.

    Per-file results are positional with the input items.
    """

    def __init__(self, results: list[CheckCollectionResult]):
        self.results: list[CheckCollectionResult] = results


class CheckCollectionImpl:
    """Engine that verifies one check-collection file against a data source.

    Subclasses provide four plain class attributes; the engine inherits.
    """

    # Subclasses MUST override these. No __init_subclass__ validator — missing
    # values fail loudly the first time .verify() runs or at Cloud upload.
    wire_source: str = ""
    display_name: str = "check collection"
    yaml_class: type = CheckCollectionYaml  # subclass overrides to its yaml type
    result_class: type = CheckCollectionResult  # subclass overrides to its result type

    # Plugin hooks. Extensions register against the concrete subclass that
    # cares about them (today: ``ContractImpl.contract_impl_extensions``);
    # the base initialises its own empty dict so the engine code can iterate
    # without conditional checks.
    contract_impl_extensions: dict[str, type] = {}

    @classmethod
    def register_extension(cls, name: str, extension_cls: type) -> None:
        cls.contract_impl_extensions[name] = extension_cls

    def __init__(
        self,
        yaml: CheckCollectionYaml,
        data_source_impl: Optional[DataSourceImpl],
        soda_cloud_impl: Optional[SodaCloud] = None,
        publish_results: bool = False,
        collection_id: Optional[str] = None,
        only_validate_without_execute: bool = False,
        check_selectors: Optional[list] = None,
        execution_timestamp: Optional[datetime] = None,
        data_timestamp: Optional[datetime] = None,
        all_data_source_impls: Optional[dict[str, DataSourceImpl]] = None,
        dwh_data_source_file_path: Optional[str] = None,
        logs: Optional[Logs] = None,
    ):
        # Defer import: CheckImpl/ColumnImpl/MetricsResolver/RowCountMetricImpl live in
        # contract_verification_impl.py which imports this module.
        from soda_core.contracts.impl.check_types.row_count_check import (
            RowCountMetricImpl,
        )
        from soda_core.contracts.impl.contract_verification_impl import MetricsResolver

        self.logs: Logs = logs if logs is not None else Logs()
        self.yaml: CheckCollectionYaml = yaml
        self.only_validate_without_execute: bool = only_validate_without_execute
        self.data_source_impl: Optional[DataSourceImpl] = data_source_impl
        self.all_data_source_impls: dict[str, DataSourceImpl] = all_data_source_impls or {}
        self.soda_cloud: Optional[SodaCloud] = soda_cloud_impl
        self.publish_results: bool = publish_results
        self.collection_id: Optional[str] = collection_id
        self.soda_config = EnvConfigHelper()

        self.filter: Optional[str] = yaml.filter
        self.check_selectors: list = check_selectors if check_selectors is not None else []

        self.started_timestamp: datetime = datetime.now(tz=timezone.utc)

        self.execution_timestamp: datetime = execution_timestamp
        self.data_timestamp: datetime = data_timestamp

        self.dataset_name: Optional[str] = None

        self.check_attributes: dict[str, Any] = yaml.check_attributes

        self.dataset_identifier = DatasetIdentifier.parse(yaml.dataset)
        self.dataset_prefix: list[str] = self.dataset_identifier.prefixes
        self.dataset_name = self.dataset_identifier.dataset_name

        self.metrics_resolver: MetricsResolver = MetricsResolver()

        self.column_impls: list = []
        self.check_impls: list = []

        # TODO replace usage of self.soda_qualified_dataset_name with self.dataset_identifier
        self.soda_qualified_dataset_name = yaml.dataset
        # TODO replace usage of self.sql_qualified_dataset_name with self.dataset_identifier
        self.sql_qualified_dataset_name: Optional[str] = None

        self.datasource_warehouse: Optional[str] = None
        self.compute_warehouse: Optional[str] = None

        if data_source_impl:
            self.sql_qualified_dataset_name = data_source_impl.sql_dialect.qualify_dataset_name(
                dataset_prefix=self.dataset_prefix, dataset_name=self.dataset_name
            )

            if data_source_impl.data_source_connection:
                if hasattr(data_source_impl.data_source_connection.connection_properties, "warehouse"):
                    self.datasource_warehouse = data_source_impl.data_source_connection.connection_properties.warehouse

                if self.datasource_warehouse is None:
                    self.datasource_warehouse = data_source_impl.get_current_warehouse()

        self.row_count_metric_impl = self.metrics_resolver.resolve_metric(RowCountMetricImpl(contract_impl=self))
        self.dataset_rows_tested: Optional[int] = None

        # Dataset defining CTE - used as basis for all queries in this collection
        self.cte = CTE("_soda_filtered_dataset").AS(
            [
                SELECT(STAR()),
                FROM(self.dataset_identifier.dataset_name, self.dataset_identifier.prefixes),
                WHERE.optional(SqlExpressionStr.optional(self.filter)),
            ]
        )
        # Optional sampler configuration.
        self.sampler_type: Optional[SamplerType] = None
        self.sampler_limit: Optional[Number] = None

        self.dataset_configuration: Optional[DatasetConfigurationDTO] = None
        if self.soda_cloud:
            self.dataset_configuration = self.soda_cloud.fetch_dataset_configuration(self.dataset_identifier)

        if self.dataset_configuration:
            if (
                self.dataset_configuration.test_row_sampler_configuration
                and self.dataset_configuration.test_row_sampler_configuration.enabled
                and self.dataset_configuration.test_row_sampler_configuration.test_row_sampler is not None
            ):
                self.sampler_type = map_sampler_type_from_dto(
                    self.dataset_configuration.test_row_sampler_configuration.test_row_sampler.type
                )
                self.sampler_limit = self.dataset_configuration.test_row_sampler_configuration.test_row_sampler.limit

            if self.dataset_configuration.compute_warehouse_override:
                self.compute_warehouse = self.dataset_configuration.compute_warehouse_override.name

        if self.should_apply_sampling:
            logger.info(
                f"Row sampling is enabled for dataset {self.dataset_identifier.to_string()} "
                f"with sampler config: type:'{self.dataset_configuration.test_row_sampler_configuration.test_row_sampler.type}', "
                f"limit:'{self.dataset_configuration.test_row_sampler_configuration.test_row_sampler.limit}'"
            )

            self.cte.cte_query[1] = self.cte.cte_query[1].SAMPLE(
                self.sampler_type,
                self.sampler_limit,
            )

        self.extensions: list = []
        for extension_cls in type(self).contract_impl_extensions.values():
            try:
                extension = extension_cls(self)
                self.extensions.append(extension)
            except Exception as e:
                logger.error(
                    f"Error extending {self.display_name} implementation with extension {extension_cls.__name__}: {e}",
                )

        self.column_impls = self._parse_columns(yaml)
        self.check_impls = self._parse_checks(yaml)

        dataset_check_impls: list = list(self.check_impls)
        column_check_impls: list = []
        for column_impl in self.column_impls:
            column_check_impls.extend(column_impl.check_impls)
        # For consistency and predictability, we want the checks eval and results in the same order as in the contract
        self.all_check_impls: list = (
            dataset_check_impls + column_check_impls
            if self._dataset_checks_came_before_columns_in_yaml()
            else column_check_impls + dataset_check_impls
        )

        self._verify_duplicate_identities(self.all_check_impls)
        self.metrics: list = self.metrics_resolver.get_resolved_metrics()

        self.queries: list = []
        if data_source_impl:
            self.queries = self._build_queries()

        self.dwh_data_source_file_path: Optional[str] = dwh_data_source_file_path

    @property
    def is_test_verification_on_agent(self) -> bool:
        """Whether this is a test scan running on the Soda Cloud agent.

        Default: False. Subclasses (``ContractImpl``) override.
        """
        return False

    @property
    def is_sampling_enabled(self) -> bool:
        return self.sampler_type is not None and self.sampler_limit is not None

    @property
    def should_apply_sampling(self) -> bool:
        return self.is_test_verification_on_agent and self.is_sampling_enabled

    def _dataset_checks_came_before_columns_in_yaml(self) -> Optional[bool]:
        keys: list[str] = self.yaml.contract_yaml_object.keys()
        if "checks" in keys and "columns" in keys:
            return keys.index("checks") < keys.index("columns")
        return None

    def _parse_checks(self, yaml: CheckCollectionYaml) -> list:
        from soda_core.contracts.impl.contract_verification_impl import CheckImpl

        check_impls: list = []
        if yaml.checks:
            for check_yaml in yaml.checks:
                if check_yaml:
                    check = CheckImpl.parse_check(contract_impl=self, check_yaml=check_yaml)
                    check_impls.append(check)

        for extension in self.extensions:
            try:
                check_impls.extend(extension.parse_checks(contract_impl=self))
            except Exception as e:
                logger.error(f"Error parsing checks with extension {extension.__class__.__name__}: {e}")

        return check_impls

    def _build_queries(self) -> list:
        from soda_core.contracts.impl.check_types.schema_check import SchemaQuery
        from soda_core.contracts.impl.contract_verification_impl import (
            AggregationMetricImpl,
            AggregationQuery,
        )

        queries: list = []

        for check in self.all_check_impls:
            queries.extend(check.queries)

        aggregation_metrics: list = []
        for metric in self.metrics:
            # Only build aggregation queries for metrics of known origin. Extensions might build their own queries.
            if isinstance(metric, AggregationMetricImpl):
                if (metric.data_source_impl is None and metric.dataset_identifier is None) or (
                    metric.data_source_impl == self.data_source_impl
                    and metric.dataset_identifier == self.dataset_identifier
                ):
                    aggregation_metrics.append(metric)

        schema_queries: list = []
        other_queries: list = []
        for query in queries:
            if isinstance(query, SchemaQuery):
                schema_queries.append(query)
            else:
                other_queries.append(query)

        aggregation_queries: list = []
        for aggregation_metric in aggregation_metrics:
            if len(aggregation_queries) == 0 or not aggregation_queries[-1].can_accept(aggregation_metric):
                aggregation_queries.append(
                    AggregationQuery(
                        cte=self.cte,
                        dataset_prefix=self.dataset_prefix,
                        dataset_name=self.dataset_name,
                        data_source_impl=self.data_source_impl,
                        logs=self.logs,
                    )
                )
            last_aggregation_query = aggregation_queries[-1]
            last_aggregation_query.append_aggregation_metric(aggregation_metric)

        all_queries: list = schema_queries + aggregation_queries + other_queries

        for extension in self.extensions:
            try:
                extension_queries: list = extension.build_queries(contract_impl=self)
                all_queries.extend(extension_queries)
            except Exception as e:
                logger.error(f"Error building queries with extension {extension.__class__.__name__}: {e}")

        return all_queries

    def _parse_columns(self, yaml: CheckCollectionYaml) -> list:
        from soda_core.contracts.impl.contract_verification_impl import ColumnImpl

        columns: list = []
        if yaml.columns:
            for column_yaml in yaml.columns:
                if column_yaml:
                    column = ColumnImpl(contract_impl=self, column_yaml=column_yaml)
                    columns.append(column)
        return columns

    def verify(self) -> CheckCollectionResult:
        from soda_core.contracts.impl.contract_verification_impl import (
            ContractVerificationHandlerRegistry,
            DerivedMetricImpl,
            MeasurementValues,
            _get_contract_verification_status,
        )

        if self.data_source_impl and self.soda_config.is_running_on_agent:
            self.data_source_impl.switch_warehouse(self.compute_warehouse, contract_impl=self)
        data_source: Optional[DataSource] = None
        check_results: list[CheckResult] = []
        measurements: list[Measurement] = []
        verification_status: ContractVerificationStatus = ContractVerificationStatus.UNKNOWN

        verb: str = "Validating" if self.only_validate_without_execute else "Verifying"
        logger.info(
            f"{verb} {self.display_name} {Emoticons.SCROLL} "
            f"{self.yaml.contract_yaml_source.file_path} {Emoticons.FINGERS_CROSSED}"
        )

        if self.data_source_impl:
            data_source = self.data_source_impl.build_data_source()

        if self.logs.has_errors:
            verification_status = ContractVerificationStatus.ERROR

        elif not self.only_validate_without_execute:
            # Executing the queries will set the value of the metrics linked to queries.
            # A SodaCoreException from one query (e.g. an aggregation referencing a column
            # that has been dropped) must not abort the scan — other queries, including the
            # schema query, still need to run so the user sees the real cause.
            for query in self.queries:
                try:
                    query_measurements: list[Measurement] = query.execute()
                    measurements.extend(query_measurements)
                except SodaCoreException as e:
                    logger.error(f"Query execution failed, continuing with remaining checks: {e}")

            measurement_values: MeasurementValues = MeasurementValues(measurements)

            self.dataset_rows_tested = measurement_values.get_value(self.row_count_metric_impl)

            # Triggering the derived metrics to initialize their value based on their dependencies
            derived_metric_impls: list = [
                derived_metric for derived_metric in self.metrics if isinstance(derived_metric, DerivedMetricImpl)
            ]
            for derived_metric_impl in derived_metric_impls:
                measurement_values.derive_value(derived_metric_impl)

            if self.data_source_impl:
                # Evaluate the checks
                for check_impl in self.all_check_impls:
                    if check_impl.skip:
                        logger.info(f"Skipping evaluation of check at path '{check_impl.path}'")
                        check_result: CheckResult = CheckResult(
                            check=check_impl._build_check_info(), outcome=CheckOutcome.EXCLUDED
                        )
                    else:
                        check_result: CheckResult = check_impl.evaluate(measurement_values=measurement_values)
                    check_results.append(check_result)

            verification_status = _get_contract_verification_status(self.logs.has_errors, check_results)

            log_lines = self.build_log_summary(
                soda_qualified_dataset_name=self.soda_qualified_dataset_name, check_results=check_results
            )
            for line in log_lines:
                logger.info(line)

        log_records: Optional[list[LogRecord]] = self.logs.pop_log_records()

        soda_cloud_file_id: Optional[str] = None
        sending_results_to_soda_cloud_failed: bool = False
        yaml_source_str_original = self.yaml.contract_yaml_source.yaml_str_original
        soda_cloud_response_json: Optional[dict] = None

        if self.soda_cloud and self.publish_results:
            soda_cloud_file_id = self.soda_cloud._upload_contract_yaml_file(yaml_source_str_original)

        post_processing_stages: list[PostProcessingStage] = []
        for handler in ContractVerificationHandlerRegistry.post_processing_stages.values():
            post_processing_stages += handler.provides_post_processing_stages()

        verification_result: CheckCollectionResult = self.result_class(
            contract=Contract(
                data_source_name=self.data_source_impl.name if self.data_source_impl else None,
                dataset_prefix=self.dataset_prefix,
                dataset_name=self.dataset_name,
                soda_qualified_dataset_name=self.soda_qualified_dataset_name,
                source=YamlFileContentInfo(
                    source_content_str=yaml_source_str_original,
                    local_file_path=self.yaml.contract_yaml_source.file_path,
                    soda_cloud_file_id=soda_cloud_file_id,
                ),
                dataset_id=None,
            ),
            data_source=data_source,
            data_timestamp=self.data_timestamp,
            started_timestamp=self.started_timestamp,
            ended_timestamp=datetime.now(tz=timezone.utc),
            measurements=measurements,
            check_results=check_results,
            sending_results_to_soda_cloud_failed=sending_results_to_soda_cloud_failed,
            status=verification_status,
            log_records=log_records,
            post_processing_stages=post_processing_stages,
        )

        scan_id: Optional[str] = None
        if soda_cloud_file_id:
            if data_source is None:
                logger.error(
                    f"Not sending results to Soda Cloud {Emoticons.CROSS_MARK} "
                    f"Data source not found. Check that the data source name in the contract's "
                    f"'dataset' field matches the name in your data source configuration."
                )
                sending_results_to_soda_cloud_failed = True
                verification_result.sending_results_to_soda_cloud_failed = True
            else:
                # send_contract_result will use contract.source.soda_cloud_file_id
                soda_cloud_response_json = self.soda_cloud.send_contract_result(
                    verification_result, wire_source=self.wire_source
                )
                scan_id = soda_cloud_response_json.get("scanId") if soda_cloud_response_json else None
                if not scan_id:
                    verification_result.sending_results_to_soda_cloud_failed = True
                else:
                    verification_result.scan_id = scan_id
                    verification_result.contract.dataset_id = self.__get_dataset_id(
                        soda_cloud_response_json, self.soda_qualified_dataset_name
                    )
        else:
            logger.debug(f"Not sending results to Soda Cloud {Emoticons.CROSS_MARK}")

        for handler in ContractVerificationHandlerRegistry.contract_verification_handlers:
            try:
                handler.handle(
                    contract_impl=self,
                    data_source_impl=self.data_source_impl,
                    contract_verification_result=verification_result,
                    soda_cloud=self.soda_cloud,
                    soda_cloud_send_results_response_json=soda_cloud_response_json,
                    dwh_data_source_file_path=self.dwh_data_source_file_path,
                )
            except Exception as e:
                logger.error(f"Error in {self.display_name} verification handler: {e}", exc_info=True)
                self._handle_post_processing_failure(scan_id=scan_id, exc=e, contract_verification_handler=handler)

        return verification_result

    def verify_on_agent(
        self,
        soda_cloud_impl: SodaCloud,
        variables: dict,
        blocking_timeout_in_minutes: int,
        publish_results: bool,
        verbose: bool,
    ) -> CheckCollectionResult:
        """Agent-path verification. Default: raise NotImplementedError.

        Subtypes that support agent execution (``ContractImpl``) override.
        """
        raise NotImplementedError(f"{self.display_name} does not support agent execution")

    @classmethod
    def build_error_result(
        cls,
        yaml_source: CheckCollectionYamlSource,
        exception: BaseException,
    ) -> CheckCollectionResult:
        """Build a minimal ERROR-status result for a file that failed before
        producing real output.

        Used by per-item isolation in ``execute_check_collections``.
        """
        now = datetime.now(tz=timezone.utc)
        result = cls.result_class(
            contract=Contract(
                data_source_name=None,
                dataset_prefix=[],
                dataset_name="",
                soda_qualified_dataset_name="",
                source=YamlFileContentInfo(
                    source_content_str=getattr(yaml_source, "yaml_str_original", None),
                    local_file_path=getattr(yaml_source, "file_path", None),
                ),
            ),
            data_source=None,
            data_timestamp=None,
            started_timestamp=now,
            ended_timestamp=now,
            status=ContractVerificationStatus.ERROR,
            measurements=[],
            check_results=[],
            sending_results_to_soda_cloud_failed=False,
            log_records=None,
            post_processing_stages=[],
        )
        result.error = exception
        return result

    def build_log_summary(self, soda_qualified_dataset_name: str, check_results: list[CheckResult]) -> list[str]:
        from tabulate import tabulate

        summary_lines: list[str] = []

        failed_count: int = 0
        warned_count: int = 0
        not_evaluated_count: int = 0
        passed_count: int = 0
        excluded_count: int = 0

        for check_result in check_results:
            if check_result.is_failed:
                failed_count += 1
            elif check_result.is_not_evaluated:
                not_evaluated_count += 1
            elif check_result.is_passed:
                passed_count += 1
            elif check_result.is_warned:
                warned_count += 1
            elif check_result.is_excluded:
                excluded_count += 1
        total_count: int = failed_count + not_evaluated_count + passed_count + warned_count + excluded_count

        error_count: int = len(self.logs.get_errors())

        table_lines = [
            ["Checks", total_count],
            ["Passed", passed_count, Emoticons.WHITE_CHECK_MARK],
        ]

        if failed_count > 0:
            table_lines.append(["Failed", failed_count, Emoticons.CROSS_MARK])
        else:
            table_lines.append(["Failed", failed_count, Emoticons.WHITE_CHECK_MARK])

        if warned_count > 0:
            table_lines.append(["Warned", warned_count, Emoticons.WARNING])
        else:
            table_lines.append(["Warned", warned_count, Emoticons.WHITE_CHECK_MARK])

        if not_evaluated_count > 0:
            table_lines.append(["Not Evaluated", not_evaluated_count, Emoticons.CROSS_MARK])
        else:
            table_lines.append(["Not Evaluated", not_evaluated_count, Emoticons.WHITE_CHECK_MARK])

        if excluded_count > 0:
            table_lines.append(["Excluded", excluded_count, Emoticons.QUESTION_MARK])
        else:
            table_lines.append(["Excluded", excluded_count, Emoticons.WHITE_CHECK_MARK])

        if error_count > 0:
            table_lines.append(["Runtime Errors", error_count, Emoticons.CROSS_MARK])
        else:
            table_lines.append(["Runtime Errors", error_count, Emoticons.WHITE_CHECK_MARK])

        summary_lines.append(f"\n### Contract results for {soda_qualified_dataset_name}")
        summary_lines.append(self.build_summary_table(check_results))

        overview_table = tabulate(table_lines, tablefmt="github", stralign="left")
        summary_lines.append(f"# Summary:\n{overview_table}\n")

        return [line for joined_line in summary_lines for line in joined_line.split("\n")]

    def build_summary_table(self, check_results: list[CheckResult]) -> str:
        from tabulate import tabulate

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
    def _verify_duplicate_identities(cls, all_check_impls: list):
        checks_by_identity: dict = {}
        for check_impl in all_check_impls:
            existing_check_impl = checks_by_identity.get(check_impl.identity)
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

    def _handle_post_processing_failure(
        self,
        scan_id: Optional[str],
        exc: Exception,
        contract_verification_handler,
    ):
        from soda_core.contracts.contract_verification import PostProcessingStageState

        if scan_id is None:
            logger.warning("Not sending post-processing stage updates to Soda Cloud - no scan ID")
            return
        if self.soda_cloud is None:
            logger.warning("Not sending post-processing stage updates to Soda Cloud - no Soda Cloud client")
            return
        for post_processing_stage in contract_verification_handler.provides_post_processing_stages():
            self.soda_cloud.post_processing_update(
                stage=post_processing_stage.name,
                scan_id=scan_id,
                state=PostProcessingStageState.FAILED,
                error=get_exception_stacktrace(exc),
            )

    def __get_dataset_id(self, soda_cloud_response_json: dict, qualified_dataset_name: str) -> Optional[str]:
        for check in soda_cloud_response_json.get("checks", []):
            for datasets in check.get("datasets", []):
                dataset_dqn: Optional[str] = datasets.get("dqn")
                if dataset_dqn and dataset_dqn == qualified_dataset_name:
                    return datasets.get("id")
        return None
