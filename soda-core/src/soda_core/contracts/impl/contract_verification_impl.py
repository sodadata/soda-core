from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import timezone
from enum import Enum
from io import StringIO

from ruamel.yaml import YAML
from soda_core.common.consistent_hash_builder import ConsistentHashBuilder
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.exceptions import InvalidContractException
from soda_core.common.logging_constants import Emoticons, ExtraKeys, soda_logger
from soda_core.common.logs import Location, Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.sql_dialect import *
from soda_core.common.yaml import (
    ContractYamlSource,
    DataSourceYamlSource,
    SodaCloudYamlSource,
    VariableResolver,
)
from soda_core.contracts.contract_verification import (
    Check,
    CheckOutcome,
    CheckResult,
    Contract,
    ContractVerificationResult,
    ContractVerificationSessionResult,
    DataSource,
    Measurement,
    Threshold,
    YamlFileContentInfo,
)
from soda_core.contracts.impl.contract_yaml import (
    CheckYaml,
    ColumnYaml,
    ContractYaml,
    MissingAncValidityCheckYaml,
    MissingAndValidityYaml,
    RangeYaml,
    RegexFormat,
    ThresholdYaml,
    ValidReferenceDataYaml,
)

logger: logging.Logger = soda_logger


class ContractVerificationSessionImpl:
    @classmethod
    def execute(
        cls,
        contract_yaml_sources: list[ContractYamlSource],
        only_validate_without_execute: bool = False,
        variables: Optional[dict[str, str]] = None,
        data_source_impls: Optional[list[DataSourceImpl]] = None,
        data_source_yaml_sources: Optional[list[DataSourceYamlSource]] = None,
        soda_cloud_yaml_source: Optional[SodaCloudYamlSource] = None,
        soda_cloud_impl: Optional[SodaCloud] = None,
        soda_cloud_publish_results: bool = False,
        soda_cloud_use_agent: bool = False,
        soda_cloud_use_agent_blocking_timeout_in_minutes: int = 60,
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

        # Validate input soda_cloud_yaml_source
        if soda_cloud_yaml_source is not None:
            assert isinstance(soda_cloud_yaml_source, SodaCloudYamlSource)

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
                soda_cloud_yaml_source=soda_cloud_yaml_source,
                soda_cloud_impl=soda_cloud_impl,
                soda_cloud_use_agent_blocking_timeout_in_minutes=soda_cloud_use_agent_blocking_timeout_in_minutes,
            )

        else:
            contract_verification_results: list[ContractVerificationResult] = cls._execute_locally(
                logs=logs,
                contract_yaml_sources=contract_yaml_sources,
                only_validate_without_execute=only_validate_without_execute,
                variables=variables,
                data_source_impls=data_source_impls,
                data_source_yaml_sources=data_source_yaml_sources,
                soda_cloud_yaml_source=soda_cloud_yaml_source,
                soda_cloud_impl=soda_cloud_impl,
                soda_cloud_publish_results=soda_cloud_publish_results,
            )
        return ContractVerificationSessionResult(contract_verification_results=contract_verification_results)

    @classmethod
    def _execute_locally(
        cls,
        logs: Logs,
        contract_yaml_sources: list[ContractYamlSource],
        only_validate_without_execute: bool,
        variables: dict[str, str],
        data_source_impls: list[DataSourceImpl],
        data_source_yaml_sources: list[DataSourceYamlSource],
        soda_cloud_yaml_source: Optional[SodaCloudYamlSource],
        soda_cloud_impl: Optional[SodaCloud],
        soda_cloud_publish_results: bool,
    ) -> list[ContractVerificationResult]:
        contract_verification_results: list[ContractVerificationResult] = []

        data_source_impls_by_name: dict[str, DataSourceImpl] = cls._build_data_source_impl_by_name(
            data_source_impls=data_source_impls, data_source_yaml_sources=data_source_yaml_sources, variables=variables
        )

        soda_cloud_impl: SodaCloud = cls._build_soda_cloud_impl(
            soda_cloud_impl=soda_cloud_impl, soda_cloud_yaml_source=soda_cloud_yaml_source, variables=variables
        )

        opened_data_sources: list[DataSourceImpl] = []
        try:
            for contract_yaml_source in contract_yaml_sources:
                try:
                    contract_yaml: ContractYaml = ContractYaml.parse(
                        contract_yaml_source=contract_yaml_source, variables=variables
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
                        data_source_impl=data_source_impl,
                        variables=variables,
                        soda_cloud=soda_cloud_impl,
                        publish_results=soda_cloud_publish_results,
                        logs=logs,
                    )
                    contract_verification_result: ContractVerificationResult = contract_impl.verify()
                    contract_verification_results.append(contract_verification_result)
                except InvalidContractException:
                    raise
                except:
                    logger.error(msg=f"Could not verify contract {contract_yaml_source}", exc_info=True)
        finally:
            for data_source_impl in opened_data_sources:
                data_source_impl.close_connection()
        return contract_verification_results

    @classmethod
    def _build_data_source_impl_by_name(
        cls,
        data_source_impls: list[DataSourceImpl],
        data_source_yaml_sources: list[DataSourceYamlSource],
        variables: dict[str, str],
    ) -> dict[str, DataSourceImpl]:
        data_source_impl_by_name: dict[str, DataSourceImpl] = (
            {data_source_impl.name: data_source_impl for data_source_impl in data_source_impls}
            if data_source_impls
            else {}
        )
        for data_source_yaml_source in data_source_yaml_sources:
            data_source_impl: DataSourceImpl = DataSourceImpl.from_yaml_source(
                data_source_yaml_source=data_source_yaml_source, variables=variables
            )
            data_source_impl_by_name[data_source_impl.name] = data_source_impl
        return data_source_impl_by_name

    @classmethod
    def _build_soda_cloud_impl(
        cls,
        soda_cloud_impl: Optional[SodaCloud],
        soda_cloud_yaml_source: Optional[SodaCloudYamlSource],
        variables: dict[str, str],
    ) -> Optional[SodaCloud]:
        if soda_cloud_impl:
            return soda_cloud_impl
        if soda_cloud_yaml_source:
            return SodaCloud.from_yaml_source(soda_cloud_yaml_source=soda_cloud_yaml_source, variables=variables)
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
        variables: Optional[dict[str, str]],
        soda_cloud_yaml_source: Optional[SodaCloudYamlSource],
        soda_cloud_impl: Optional[SodaCloud],
        soda_cloud_use_agent_blocking_timeout_in_minutes,
    ) -> list[ContractVerificationResult]:
        contract_verification_results: list[ContractVerificationResult] = []

        soda_cloud_impl: SodaCloud = cls._build_soda_cloud_impl(
            soda_cloud_impl=soda_cloud_impl, soda_cloud_yaml_source=soda_cloud_yaml_source, variables=variables
        )

        for contract_yaml_source in contract_yaml_sources:
            try:
                contract_yaml: ContractYaml = ContractYaml.parse(
                    contract_yaml_source=contract_yaml_source, variables=variables
                )
                contract_verification_result: ContractVerificationResult = soda_cloud_impl.verify_contract_on_agent(
                    contract_yaml=contract_yaml,
                    blocking_timeout_in_minutes=soda_cloud_use_agent_blocking_timeout_in_minutes,
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
        data_source_impl: DataSourceImpl,
        variables: dict[str, str],
        soda_cloud: Optional[SodaCloud],
        publish_results: bool,
    ):
        self.logs: Logs = logs
        self.contract_yaml: ContractYaml = contract_yaml
        self.only_validate_without_execute: bool = only_validate_without_execute
        self.data_source_impl: DataSourceImpl = data_source_impl
        self.variables: dict[str, str] = variables
        self.soda_cloud: Optional[SodaCloud] = soda_cloud
        self.publish_results: bool = publish_results

        self.started_timestamp: datetime = datetime.now(tz=timezone.utc)
        # self.data_timestamp can be None if the user specified a DATA_TS variable that is not in the correct format
        self.data_timestamp: Optional[datetime] = self._get_data_timestamp(
            variables=variables, default=self.started_timestamp
        )

        self.dataset_prefix: Optional[list[str]] = None
        self.dataset_name: Optional[str] = None

        dataset_identifier = DatasetIdentifier.parse(contract_yaml.dataset)
        self.dataset_prefix = dataset_identifier.prefixes
        self.dataset_name = dataset_identifier.dataset_name

        self.metrics_resolver: MetricsResolver = MetricsResolver()

        self.column_impls: list[ColumnImpl] = []
        self.check_impls: list[CheckImpl] = []

        self.soda_qualified_dataset_name = contract_yaml.dataset

        self.sql_qualified_dataset_name: Optional[str] = (
            data_source_impl.sql_dialect.qualify_dataset_name(
                dataset_prefix=self.dataset_prefix, dataset_name=self.dataset_name
            )
            if data_source_impl
            else None
        )

        self.column_impls: list[ColumnImpl] = self._parse_columns(contract_yaml=contract_yaml)
        self.check_impls: list[CheckImpl] = self._parse_checks(contract_yaml)

        self.all_check_impls: list[CheckImpl] = list(self.check_impls)
        for column_impl in self.column_impls:
            self.all_check_impls.extend(column_impl.check_impls)

        self._verify_duplicate_identities(self.all_check_impls)

        self.metrics: list[MetricImpl] = self.metrics_resolver.get_resolved_metrics()
        self.queries: list[Query] = self._build_queries() if data_source_impl else []

    def _get_data_timestamp(self, variables: dict[str, str], default: datetime) -> Optional[datetime]:
        now_variable_name: str = "DATA_TS"
        now_variable_timestamp_text = VariableResolver.get_variable(
            namespace="var", variables=variables, variable=now_variable_name
        )
        if isinstance(now_variable_timestamp_text, str):
            try:
                now_variable_timestamp = datetime.fromisoformat(now_variable_timestamp_text)
                if isinstance(now_variable_timestamp, datetime):
                    return now_variable_timestamp
            except:
                pass
            logger.error(
                f"Could not parse variable {now_variable_name} " f"as a timestamp: {now_variable_timestamp_text}"
            )
        else:
            return default
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
                        filter=self.contract_yaml.filter,
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
        contract: Contract = self.build_contract()
        data_source: Optional[DataSource] = None
        check_results: list[CheckResult] = []
        measurements: list[Measurement] = []

        verb: str = "Validating" if self.only_validate_without_execute else "Verifying"
        logger.info(
            f"{verb} contract {Emoticons.SCROLL} "
            f"{self.contract_yaml.contract_yaml_source.file_path} {Emoticons.FINGERS_CROSSED}"
        )

        if not self.logs.has_errors():
            # Executing the queries will set the value of the metrics linked to queries
            if not self.only_validate_without_execute:
                for query in self.queries:
                    query_measurements: list[Measurement] = query.execute()
                    measurements.extend(query_measurements)

            # Triggering the derived metrics to initialize their value based on their dependencies
            derived_metric_impls: list[DerivedPercentageMetricImpl] = [
                derived_metric
                for derived_metric in self.metrics
                if isinstance(derived_metric, DerivedPercentageMetricImpl)
            ]
            measurement_values: MeasurementValues = MeasurementValues(measurements)
            for derived_metric_impl in derived_metric_impls:
                derived_measurement: Measurement = derived_metric_impl.create_derived_measurement(measurement_values)
                if isinstance(derived_measurement, Measurement):
                    measurements.append(derived_measurement)

            if self.data_source_impl:
                data_source = self.data_source_impl.build_data_source()

                # Evaluate the checks
                measurement_values: MeasurementValues = MeasurementValues(measurements)
                for check_impl in self.all_check_impls:
                    check_result: CheckResult = check_impl.evaluate(
                        measurement_values=measurement_values, contract=contract
                    )
                    check_results.append(check_result)

        contract_verification_result: ContractVerificationResult = ContractVerificationResult(
            contract=contract,
            data_source=data_source,
            data_timestamp=self.data_timestamp,
            started_timestamp=self.started_timestamp,
            ended_timestamp=datetime.now(tz=timezone.utc),
            measurements=measurements,
            check_results=check_results,
            sending_results_to_soda_cloud_failed=False,
        )

        if not self.only_validate_without_execute:
            self.log_summary(contract_verification_result)

        contract_verification_result.log_records = self.logs.pop_log_records()

        if self.soda_cloud and self.publish_results:
            # upload_contract_file fills in contract.source.soda_cloud_file_id if all goes well
            self.soda_cloud.upload_contract_file(contract_verification_result.contract)
            # send_contract_result will use contract.source.soda_cloud_file_id
            response_ok: bool = self.soda_cloud.send_contract_result(contract_verification_result)
            if not response_ok:
                contract_verification_result.sending_results_to_soda_cloud_failed = True
        else:
            logger.debug(f"Not sending results to Soda Cloud {Emoticons.CROSS_MARK}")

        return contract_verification_result

    def log_summary(self, contract_verification_result: ContractVerificationResult):
        logger.info(f"### Contract results for {contract_verification_result.contract.soda_qualified_dataset_name}")
        failed_count: int = 0
        not_evaluated_count: int = 0
        passed_count: int = 0
        for check_result in contract_verification_result.check_results:
            check_result.log_summary(self.logs)
            if check_result.outcome == CheckOutcome.FAILED:
                failed_count += 1
            elif check_result.outcome == CheckOutcome.NOT_EVALUATED:
                not_evaluated_count += 1
            elif check_result.outcome == CheckOutcome.PASSED:
                passed_count += 1

        error_count: int = len(self.logs.get_errors())

        not_evaluated_count: int = sum(
            1 if check_result.outcome == CheckOutcome.NOT_EVALUATED else 0
            for check_result in contract_verification_result.check_results
        )

        if failed_count + error_count + not_evaluated_count == 0:
            logger.info(f"Contract summary: All is good. All {passed_count} checks passed. No execution errors.")
        else:
            logger.info(
                f"Contract summary: Ouch! {failed_count} checks failures, "
                f"{passed_count} checks passed, {not_evaluated_count} checks not evaluated "
                f"and {error_count} errors."
            )

    def build_contract(self) -> Contract:
        return Contract(
            data_source_name=self.data_source_impl.name if self.data_source_impl else None,
            dataset_prefix=self.dataset_prefix,
            dataset_name=self.dataset_name,
            soda_qualified_dataset_name=self.soda_qualified_dataset_name,
            source=YamlFileContentInfo(
                source_content_str=self.contract_yaml.contract_yaml_source.yaml_str_original,
                local_file_path=self.contract_yaml.contract_yaml_source.file_path,
            ),
        )

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


class MeasurementValues:
    def __init__(self, measurements: list[Measurement]):
        self.measurement_values_by_metric_id: dict[str, any] = {
            measurement.metric_id: measurement.value for measurement in measurements
        }

    def get_value(self, metric_impl: MetricImpl) -> any:
        return self.measurement_values_by_metric_id.get(metric_impl.id)


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
    def __init__(self, valid_reference_data_yaml: ValidReferenceDataYaml):
        self.dataset_name: Optional[str] = None
        self.dataset_prefix: str | list[str] | None = None
        if isinstance(valid_reference_data_yaml.dataset, str):
            self.dataset_name = valid_reference_data_yaml.dataset
        if isinstance(valid_reference_data_yaml.dataset, list) and len(valid_reference_data_yaml.dataset) > 0:
            self.dataset_name = valid_reference_data_yaml.dataset[-1]
            self.dataset_prefix = valid_reference_data_yaml.dataset[1:]
        self.column: Optional[str] = valid_reference_data_yaml.column


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
        self.valid_reference_data: Optional[ValidReferenceData] = (
            ValidReferenceData(missing_and_validity_yaml.valid_reference_data)
            if missing_and_validity_yaml.valid_reference_data
            else None
        )

    def get_missing_count_condition(self, column_name):
        is_missing_clauses: list[SqlExpression] = [IS_NULL(column_name)]
        if isinstance(self.missing_values, list):
            literal_values = [LITERAL(value) for value in self.missing_values]
            is_missing_clauses.append(IN(column_name, literal_values))
        if isinstance(self.missing_format, RegexFormat) and isinstance(self.missing_format.regex, str):
            is_missing_clauses.append(REGEX_LIKE(column_name, self.missing_format.regex))
        return OR(is_missing_clauses)

    def get_sum_missing_count_expr(self, column_name: str) -> SqlExpression:
        missing_count_condition: SqlExpression = self.get_missing_count_condition(column_name)
        return SUM(CASE_WHEN(missing_count_condition, LITERAL(1), LITERAL(0)))

    def get_invalid_count_condition(self, column_name: str) -> SqlExpression:
        invalid_clauses: list[SqlExpression] = []
        if isinstance(self.valid_values, list):
            literal_values = [LITERAL(value) for value in self.valid_values if value is not None]
            if None in self.valid_values:
                invalid_clauses.append(AND([NOT(IN(column_name, literal_values)), IS_NOT_NULL(column_name)]))
            else:
                invalid_clauses.append(NOT(IN(column_name, literal_values)))
        if isinstance(self.invalid_values, list):
            literal_values = [LITERAL(value) for value in self.invalid_values if value is not None]
            if None in self.invalid_values:
                invalid_clauses.append(AND([IN(column_name, literal_values), IS_NULL(column_name)]))
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
        missing_expr: SqlExpression = self.get_missing_count_condition(column_name)
        invalid_expression: SqlExpression = OR(invalid_clauses)
        return AND([NOT(missing_expr), OR(invalid_expression)])

    def get_sum_invalid_count_expr(self, column_name: str) -> SqlExpression:
        not_missing_and_invalid_expr = self.get_invalid_count_condition(column_name)
        return SUM(CASE_WHEN(not_missing_and_invalid_expr, LITERAL(1), LITERAL(0)))

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

        total_config_count: int = cls.__config_count(
            [
                threshold_yaml.must_be_greater_than,
                threshold_yaml.must_be_greater_than_or_equal,
                threshold_yaml.must_be_less_than,
                threshold_yaml.must_be_less_than_or_equal,
                threshold_yaml.must_be,
                threshold_yaml.must_not_be,
                threshold_yaml.must_be_between,
                threshold_yaml.must_be_not_between,
            ]
        )

        if total_config_count == 0:
            if default_threshold:
                return default_threshold
            logger.error(f"Threshold required, but not specified")
            return None

        if (
            total_config_count == 1
            and cls.__config_count(
                [
                    threshold_yaml.must_be_greater_than,
                    threshold_yaml.must_be_greater_than_or_equal,
                    threshold_yaml.must_be_less_than,
                    threshold_yaml.must_be_less_than_or_equal,
                    threshold_yaml.must_be,
                    threshold_yaml.must_not_be,
                ]
            )
            == 1
        ):
            return ThresholdImpl(
                type=ThresholdType.SINGLE_COMPARATOR,
                must_be_greater_than=threshold_yaml.must_be_greater_than,
                must_be_greater_than_or_equal=threshold_yaml.must_be_greater_than_or_equal,
                must_be_less_than=threshold_yaml.must_be_less_than,
                must_be_less_than_or_equal=threshold_yaml.must_be_less_than_or_equal,
                must_be=threshold_yaml.must_be,
                must_not_be=threshold_yaml.must_not_be,
            )

        elif total_config_count == 1 and isinstance(threshold_yaml.must_be_between, RangeYaml):
            if isinstance(threshold_yaml.must_be_between.lower_bound, Number) and isinstance(
                threshold_yaml.must_be_between.upper_bound, Number
            ):
                if threshold_yaml.must_be_between.lower_bound < threshold_yaml.must_be_between.upper_bound:
                    return ThresholdImpl(
                        type=ThresholdType.INNER_RANGE,
                        must_be_greater_than_or_equal=threshold_yaml.must_be_between.lower_bound,
                        must_be_less_than_or_equal=threshold_yaml.must_be_between.upper_bound,
                    )
                else:
                    logger.error(f"Threshold must_be_between range: " "first value must be less than the second value")
                    return None

        elif total_config_count == 1 and isinstance(threshold_yaml.must_be_not_between, RangeYaml):
            if isinstance(threshold_yaml.must_be_not_between.lower_bound, Number) and isinstance(
                threshold_yaml.must_be_not_between.upper_bound, Number
            ):
                if threshold_yaml.must_be_not_between.lower_bound < threshold_yaml.must_be_not_between.upper_bound:
                    return ThresholdImpl(
                        type=ThresholdType.OUTER_RANGE,
                        must_be_greater_than_or_equal=threshold_yaml.must_be_not_between.upper_bound,
                        must_be_less_than_or_equal=threshold_yaml.must_be_not_between.lower_bound,
                    )
                else:
                    logger.error(
                        f"Threshold must_be_not_between range: " "first value must be less than the second value"
                    )
                    return None
        else:
            lower_bound_count = cls.__config_count(
                [threshold_yaml.must_be_greater_than, threshold_yaml.must_be_greater_than_or_equal]
            )
            upper_bound_count = cls.__config_count(
                [threshold_yaml.must_be_less_than, threshold_yaml.must_be_less_than_or_equal]
            )
            if lower_bound_count == 1 and upper_bound_count == 1:
                lower_bound = (
                    threshold_yaml.must_be_greater_than
                    if threshold_yaml.must_be_greater_than is not None
                    else threshold_yaml.must_be_greater_than_or_equal
                )
                upper_bound = (
                    threshold_yaml.must_be_less_than
                    if threshold_yaml.must_be_less_than is not None
                    else threshold_yaml.must_be_less_than_or_equal
                )
                if lower_bound < upper_bound:
                    return ThresholdImpl(
                        type=ThresholdType.INNER_RANGE,
                        must_be_greater_than=threshold_yaml.must_be_greater_than,
                        must_be_greater_than_or_equal=threshold_yaml.must_be_greater_than_or_equal,
                        must_be_less_than=threshold_yaml.must_be_less_than,
                        must_be_less_than_or_equal=threshold_yaml.must_be_less_than_or_equal,
                    )
                else:
                    return ThresholdImpl(
                        type=ThresholdType.OUTER_RANGE,
                        must_be_greater_than=threshold_yaml.must_be_greater_than,
                        must_be_greater_than_or_equal=threshold_yaml.must_be_greater_than_or_equal,
                        must_be_less_than=threshold_yaml.must_be_less_than,
                        must_be_less_than_or_equal=threshold_yaml.must_be_less_than_or_equal,
                    )

    @classmethod
    def __config_count(cls, members: list[any]) -> int:
        return sum([0 if v is None else 1 for v in members])

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
        self.column_impl: Optional[ColumnImpl] = column_impl
        self.type: str = check_yaml.type_name
        self.name: Optional[str] = check_yaml.name if check_yaml.name else self.type
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

    def _resolve_metric(self, metric_impl: MetricImpl) -> MetricImpl:
        resolved_metric_impl: MetricImpl = self.contract_impl.metrics_resolver.resolve_metric(metric_impl)
        self.metrics.append(resolved_metric_impl)
        return resolved_metric_impl

    @abstractmethod
    def evaluate(self, measurement_values: MeasurementValues, contract: Contract) -> CheckResult:
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
        text_stream = StringIO()
        yaml = YAML()
        yaml.dump(self.check_yaml.check_yaml_object.to_dict(), text_stream)
        text_stream.seek(0)
        return text_stream.read()

    def _build_threshold(self) -> Optional[Threshold]:
        return self.threshold.to_threshold_info() if self.threshold else None


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
    ):
        # TODO id of a metric will have to be extended to include check configuration parameters that
        #      influence the metric identity
        self.id: str = self.create_metric_id(contract_impl, metric_type, column_impl)
        self.contract_impl: ContractImpl = contract_impl
        self.column_impl: Optional[ColumnImpl] = column_impl
        self.type: str = metric_type

    @classmethod
    def create_metric_id(
        cls,
        contract_impl: ContractImpl,
        metric_type: str,
        column_impl: Optional[ColumnImpl] = None,
    ):
        id_parts: list[str] = []
        if contract_impl.data_source_impl:
            id_parts.append(contract_impl.data_source_impl.name)
        if contract_impl.dataset_prefix:
            id_parts.extend(contract_impl.dataset_prefix)
        if column_impl:
            id_parts.append(column_impl.column_yaml.name)
        id_parts.append(metric_type)
        return "/" + "/".join(id_parts)

    def __eq__(self, other):
        if type(other) != type(self):
            return False
        return self.id == other.id


class AggregationMetricImpl(MetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        metric_type: str,
        column_impl: Optional[ColumnImpl] = None,
    ):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            metric_type=metric_type,
        )

    @abstractmethod
    def sql_expression(self) -> SqlExpression:
        pass

    def convert_db_value(self, value: any) -> any:
        return value

    def get_short_description(self) -> str:
        return self.type


class DerivedPercentageMetricImpl(MetricImpl):
    def __init__(self, metric_type: str, fraction_metric_impl: MetricImpl, total_metric_impl: MetricImpl):
        super().__init__(
            contract_impl=fraction_metric_impl.contract_impl,
            column_impl=fraction_metric_impl.column_impl,
            metric_type=metric_type,
        )
        self.fraction_metric_impl: MetricImpl = fraction_metric_impl
        self.total_metric_impl: MetricImpl = total_metric_impl

    def create_derived_measurement(self, measurement_values: MeasurementValues) -> Measurement:
        fraction: Number = measurement_values.get_value(self.fraction_metric_impl)
        total: Number = measurement_values.get_value(self.total_metric_impl)
        if isinstance(fraction, Number) and isinstance(total, Number):
            value: float = (fraction * 100 / total) if total != 0 else 0
            return Measurement(metric_id=self.id, value=value, metric_name=self.type)


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
            logger.error(msg=f"Could not execute aggregation query {sql}: {e}", exc_info=True)
            return []

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
