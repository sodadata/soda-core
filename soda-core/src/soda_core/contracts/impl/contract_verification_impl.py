from __future__ import annotations

import os
from abc import abstractmethod, ABC
from datetime import timezone
from enum import Enum
from typing import Optional

from ruamel.yaml import YAML, StringIO

from soda_core.common.consistent_hash_builder import ConsistentHashBuilder
from soda_core.common.data_source import DataSource
from soda_core.common.data_source_parser import DataSourceParser
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logs import Logs, Emoticons
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.sql_dialect import *
from soda_core.common.yaml import YamlSource, VariableResolver, YamlFileContent
from soda_core.contracts.contract_verification import ContractVerificationResult, ContractResult, \
    CheckResult, Measurement, Threshold, Contract, Check, YamlFileContentInfo, DataSourceInfo, CheckOutcome
from soda_core.contracts.impl.contract_yaml import ContractYaml, CheckYaml, ColumnYaml, RangeYaml, \
    MissingAndValidityYaml, ValidReferenceDataYaml, MissingAncValidityCheckYaml, ThresholdCheckYaml


class DataSourceContracts:

    def __init__(self, data_source: DataSource):
        self.data_source: DataSource = data_source
        self.contract_impls: list[ContractImpl] = []

    def add_contract(self, contract_impl: ContractImpl) -> None:
        self.contract_impls.append(contract_impl)


class ContractVerificationImpl:

    def __init__(
            self,
            contract_yaml_sources: list[YamlSource],
            data_source: DataSource | None,
            data_source_yaml_source: Optional[YamlSource],
            soda_cloud: 'SodaCloud' | None,
            soda_cloud_yaml_source: Optional[YamlSource],
            variables: dict[str, str],
            skip_publish: bool,
            use_agent: bool,
            logs: Logs = Logs(),
    ):
        self.logs: Logs = logs
        self.skip_publish: bool = skip_publish
        self.use_agent: bool = use_agent

        self.data_source: DataSource | None = None
        if not use_agent:
            if data_source is not None:
                self.data_source = data_source
            elif data_source_yaml_source is not None:
                data_source_yaml_file_content: YamlFileContent = data_source_yaml_source.parse_yaml_file_content(
                    file_type="data source",
                    variables=variables,
                    logs=logs
                )
                data_source_parser: DataSourceParser = DataSourceParser(data_source_yaml_file_content)
                self.data_source = data_source_parser.parse()
            if self.data_source is None:
                self.logs.error(f"No data source configured {Emoticons.POLICE_CAR_LIGHT}")
        elif data_source is not None or data_source_yaml_source is not None:
            self.logs.error(
                f"When executing the contract verification on Soda Agent, "
                f"a data source should not be configured {Emoticons.POLICE_CAR_LIGHT}"
            )

        self.soda_cloud: SodaCloud | None = soda_cloud
        if self.soda_cloud is None and soda_cloud_yaml_source is not None:
            soda_cloud_yaml_file_content: YamlFileContent = soda_cloud_yaml_source.parse_yaml_file_content(
                file_type="soda cloud",
                variables=variables,
                logs=logs
            )
            self.soda_cloud = SodaCloud.from_file(soda_cloud_yaml_file_content)

        self.contract_yamls: list[ContractYaml] = []
        self.contract_impls: list[ContractImpl] = []
        if contract_yaml_sources is None or len(contract_yaml_sources) == 0:
            self.logs.error(f"No contracts configured {Emoticons.POLICE_CAR_LIGHT}")
        else:
            for contract_yaml_source in contract_yaml_sources:
                contract_yaml: ContractYaml = ContractYaml.parse(
                    contract_yaml_source=contract_yaml_source,
                    variables=variables,
                    logs=logs
                )
                self.contract_yamls.append(contract_yaml)
        if self.data_source:
            for contract_yaml in self.contract_yamls:
                contract_impl: ContractImpl = ContractImpl(
                    contract_yaml=contract_yaml,
                    data_source=self.data_source,
                    variables=variables,
                    logs=logs
                )
                if contract_impl:
                    self.contract_impls.append(contract_impl)

    def execute(self) -> ContractVerificationResult:
        contract_results: list[ContractResult] = []

        if self.use_agent:
            contract_results = self.verify_contracts_on_agent(self.contract_yamls)
        elif self.data_source:
            contract_results = self.verify_contracts_locally(self.contract_impls, self.data_source)

        return ContractVerificationResult(
            logs=self.logs,
            contract_results=contract_results
        )

    def verify_contracts_on_agent(self, contract_yamls: list[ContractYaml]) -> list[ContractResult]:
        if self.soda_cloud and isinstance(contract_yamls, list) and len(contract_yamls) > 0:
            return self.soda_cloud.execute_contracts_on_agent(contract_yamls)
        else:
            self.logs.error(
                f"Using the agent requires a Soda Cloud configuration {Emoticons.POLICE_CAR_LIGHT}"
            )
            return []

    def verify_contracts_locally(
        self,
        contract_impls: list[ContractImpl],
        data_source: DataSource
    ) -> list[ContractResult]:
        contract_results: list[ContractResult] = []
        if self.data_source and isinstance(contract_impls, list) and len(contract_impls) > 0:
            open_close: bool = not data_source.has_open_connection()
            if open_close:
                data_source.open_connection()
            try:
                for contract_impl in contract_impls:
                    contract_result: ContractResult = contract_impl.verify()
                    contract_results.append(contract_result)
                    if self.soda_cloud:
                        self.soda_cloud.send_contract_result(contract_result, self.skip_publish)
                    else:
                        self.logs.debug(f"Not sending results to Soda Cloud {Emoticons.CROSS_MARK}")

                    self._log_summary(contract_result)
            finally:
                if open_close:
                    data_source.close_connection()
        return contract_results

    def _log_summary(self, contract_result: ContractResult):
        self.logs.info(f"### Contract results for {contract_result.contract.soda_qualified_dataset_name}")
        failed_count: int = 0
        not_evaluated_count: int = 0
        passed_count: int = 0
        for check_result in contract_result.check_results:
            check_result.log_summary(self.logs)
            if check_result.outcome == CheckOutcome.FAILED:
                failed_count += 1
            elif check_result.outcome == CheckOutcome.NOT_EVALUATED:
                not_evaluated_count += 1
            elif check_result.outcome == CheckOutcome.PASSED:
                passed_count += 1

        error_count: int = len(self.logs.get_errors())

        not_evaluated_count: int = sum(1 if check_result.outcome == CheckOutcome.NOT_EVALUATED else 0
                                       for check_result in contract_result.check_results)

        if failed_count + error_count + not_evaluated_count == 0:
            self.logs.info(f"Contract summary: All is good. All {passed_count} checks passed. No execution errors.")
        else:
            self.logs.info(
                f"Contract summary: Ouch! {failed_count} checks failures, "
                f"{passed_count} checks passed, {not_evaluated_count} checks not evaluated "
                f"and {error_count} errors."
            )


class ContractImpl:

    def __init__(
        self,
        contract_yaml: ContractYaml,
        data_source: DataSource,
        variables: dict[str, str],
        logs: Logs
    ):
        self.logs: Logs = logs
        self.data_source: DataSource = data_source
        self.contract_yaml: ContractYaml = contract_yaml
        self.variables: dict[str, str] = variables

        self.started_timestamp: datetime = datetime.now(tz=timezone.utc)
        # self.data_timestamp can be None if the user specified a DATA_TS variable that is not in the correct format
        self.data_timestamp: datetime | None = self._get_data_timestamp(
            variables=variables, default=self.started_timestamp
        )

        self.dataset_prefix: list[str] | None = self.contract_yaml.dataset_prefix
        self.dataset_name: str | None = contract_yaml.dataset if contract_yaml else None

        self.soda_qualified_dataset_name: str = self.create_soda_qualified_dataset_name(
            data_source_name=self.data_source.name,
            dataset_prefix=self.dataset_prefix,
            dataset_name=self.dataset_name
        )
        self.sql_qualified_dataset_name: str = data_source.sql_dialect.qualify_dataset_name(
            dataset_prefix=self.dataset_prefix,
            dataset_name=self.dataset_name
        )
        self.metrics_resolver: MetricsResolver = MetricsResolver()
        self.column_impls: list[ColumnImpl] = self._parse_columns(
            contract_yaml=contract_yaml,
        )

        self.check_impls: list[CheckImpl] = self._parse_checks(contract_yaml)

        self.all_check_impls: list[CheckImpl] = list(self.check_impls)
        for column_impl in self.column_impls:
            self.all_check_impls.extend(column_impl.check_impls)

        self._verify_duplicate_identities(self.all_check_impls, self.logs)

        self.metrics: list[MetricImpl] = self.metrics_resolver.get_resolved_metrics()
        self.queries: list[Query] = self._build_queries()

    def _get_data_timestamp(self, variables: dict[str, str], default: datetime) -> datetime | None:
        now_variable_name: str = "DATA_TS"
        now_variable_timestamp_text = VariableResolver.get_variable(variables=variables, variable=now_variable_name)
        if isinstance(now_variable_timestamp_text, str):
            try:
                now_variable_timestamp = datetime.fromisoformat(now_variable_timestamp_text)
                if isinstance(now_variable_timestamp, datetime):
                    return now_variable_timestamp
            except:
                pass
            self.logs.error(
                f"Could not parse variable {now_variable_name} as a timestamp: {now_variable_timestamp_text}"
            )
        else:
            return default
        return None

    @classmethod
    def create_soda_qualified_dataset_name(
        cls,
        data_source_name: str,
        dataset_prefix: list[str],
        dataset_name: str
    ) -> str:
        soda_name_parts: list[str] = [data_source_name]
        if dataset_prefix:
            soda_name_parts.extend(dataset_prefix)
        soda_name_parts.append(dataset_name)
        soda_name_parts = [str(p) for p in soda_name_parts]
        return "/" + "/".join(soda_name_parts)

    def _parse_checks(
        self,
        contract_yaml: ContractYaml
    ) -> list[CheckImpl]:
        check_impls: list[CheckImpl] = []
        if contract_yaml.checks:
            for check_yaml in contract_yaml.checks:
                if check_yaml:
                    check = CheckImpl.parse_check(
                        contract_impl=self,
                        check_yaml=check_yaml,
                        metrics_resolver=self.metrics_resolver,
                    )
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
                aggregation_queries.append(AggregationQuery(
                    dataset_prefix=self.dataset_prefix,
                    dataset_name=self.dataset_name,
                    filter_condition=None,
                    data_source=self.data_source
                ))
            last_aggregation_query: AggregationQuery = aggregation_queries[-1]
            last_aggregation_query.append_aggregation_metric(aggregation_metric)

        return schema_queries + aggregation_queries + other_queries

    def _parse_columns(self, contract_yaml: ContractYaml) -> list[ColumnImpl]:
        columns: list[ColumnImpl] = []
        if contract_yaml.columns:
            for column_yaml in contract_yaml.columns:
                column = ColumnImpl(
                    contract_impl=self,
                    column_yaml=column_yaml,
                    metrics_resolver=self.metrics_resolver
                )
                columns.append(column)
        return columns

    def verify(self) -> ContractResult:
        self.logs.info(f"Verifying {Emoticons.SCROLL} contract {self.contract_yaml.contract_yaml_file_content.yaml_file_path} {Emoticons.FINGERS_CROSSED}")

        measurements: list[Measurement] = []
        # Executing the queries will set the value of the metrics linked to queries
        for query in self.queries:
            query_measurements: list[Measurement] = query.execute()
            measurements.extend(query_measurements)

        # Triggering the derived metrics to initialize their value based on their dependencies
        derived_metric_impls: list[DerivedPercentageMetricImpl] = [
            derived_metric for derived_metric in self.metrics
            if isinstance(derived_metric, DerivedPercentageMetricImpl)
        ]
        measurement_values: MeasurementValues = MeasurementValues(measurements)
        for derived_metric_impl in derived_metric_impls:
            derived_measurement: Measurement = derived_metric_impl.create_derived_measurement(measurement_values)
            measurements.append(derived_measurement)

        contract_info: Contract = self.build_contract_info()
        data_source_info: DataSourceInfo = self.data_source.build_data_source_info()

        # Evaluate the checks
        measurement_values = MeasurementValues(measurements)
        check_results: list[CheckResult] = []
        for check_impl in self.all_check_impls:
            check_result: CheckResult = check_impl.evaluate(
                measurement_values=measurement_values,
                contract_info=contract_info
            )
            check_results.append(check_result)

        return ContractResult(
            contract=contract_info,
            data_source_info=data_source_info,
            data_timestamp=self.data_timestamp,
            started_timestamp=self.started_timestamp,
            ended_timestamp=datetime.now(tz=timezone.utc),
            measurements=measurements,
            check_results=check_results,
            logs=self.logs
        )

    def build_contract_info(self) -> Contract:
        return Contract(
            data_source_name=self.data_source.name,
            dataset_prefix=self.dataset_prefix,
            dataset_name=self.dataset_name,
            soda_qualified_dataset_name=self.soda_qualified_dataset_name,
            source=YamlFileContentInfo(
                source_content_str=self.contract_yaml.contract_yaml_file_content.yaml_str_source,
                local_file_path=self.contract_yaml.contract_yaml_file_content.yaml_file_path,
            )
        )

    @classmethod
    def _verify_duplicate_identities(cls, all_check_impls: list[CheckImpl], logs: Logs):
        checks_by_identity: dict[str, CheckImpl] = {}
        for check_impl in all_check_impls:
            existing_check_impl: CheckImpl | None = checks_by_identity.get(check_impl.identity)
            if existing_check_impl:
                # TODO distill better diagnostic error message: which check in which column on which lines in the file
                logs.error(f"Duplicate identity ({check_impl.identity})")
            checks_by_identity[check_impl.identity] = check_impl


class MeasurementValues:
    def __init__(self, measurements: list[Measurement]):
        self.measurement_values_by_metric_id: dict[str, any] = {
            measurement.metric_id: measurement.value
            for measurement in measurements
        }

    def get_value(self, metric_impl: MetricImpl) -> any:
        return self.measurement_values_by_metric_id.get(metric_impl.id)


class ColumnImpl:
    def __init__(self, contract_impl: ContractImpl, column_yaml: ColumnYaml, metrics_resolver: MetricsResolver):
        self.column_yaml = column_yaml
        self.missing_and_validity: MissingAndValidity = MissingAndValidity(
            missing_and_validity_yaml=column_yaml,
            data_source=contract_impl.data_source
        )
        self.check_impls: list[CheckImpl] = []
        if column_yaml.check_yamls:
            for check_yaml in column_yaml.check_yamls:
                if check_yaml:
                    check = CheckImpl.parse_check(
                        contract_impl=contract_impl,
                        column_impl=self,
                        check_yaml=check_yaml,
                        metrics_resolver=metrics_resolver,
                    )
                    self.check_impls.append(check)


class ValidReferenceData:

    def __init__(self, valid_reference_data_yaml: ValidReferenceDataYaml):
        self.dataset_name: str | None = None
        self.dataset_prefix: str | list[str] | None = None
        if isinstance(valid_reference_data_yaml.dataset, str):
            self.dataset_name = valid_reference_data_yaml.dataset
        if isinstance(valid_reference_data_yaml.dataset, list) and len(valid_reference_data_yaml.dataset) > 0:
            self.dataset_name = valid_reference_data_yaml.dataset[-1]
            self.dataset_prefix = valid_reference_data_yaml.dataset[1:]
        self.column: str | None = valid_reference_data_yaml.column


class MissingAndValidity:

    def __init__(self, missing_and_validity_yaml: MissingAndValidityYaml, data_source: DataSource):
        self.missing_values: list | None = missing_and_validity_yaml.missing_values
        self.missing_regex_sql: str | None = missing_and_validity_yaml.missing_regex_sql

        self.invalid_values: list | None = missing_and_validity_yaml.invalid_values
        self.invalid_format: str | None = missing_and_validity_yaml.invalid_format
        self.invalid_format_regex: str | None = data_source.get_format_regex(self.invalid_format)
        self.invalid_regex_sql: str | None = missing_and_validity_yaml.invalid_regex_sql
        self.valid_values: list | None = missing_and_validity_yaml.valid_values
        self.valid_format: str | None = missing_and_validity_yaml.valid_format
        self.valid_format_regex: str | None = data_source.get_format_regex(self.valid_format)
        self.valid_regex_sql: str | None = missing_and_validity_yaml.valid_regex_sql
        self.valid_min: Number | None = missing_and_validity_yaml.valid_min
        self.valid_max: Number | None = missing_and_validity_yaml.valid_max
        self.valid_length: int | None = missing_and_validity_yaml.valid_length
        self.valid_min_length: int | None = missing_and_validity_yaml.valid_min_length
        self.valid_max_length: int | None = missing_and_validity_yaml.valid_max_length
        self.valid_reference_data: ValidReferenceData | None = (
            ValidReferenceData(missing_and_validity_yaml.valid_reference_data)
            if missing_and_validity_yaml.valid_reference_data
            else None
        )

    def get_missing_count_condition(self, column_name):
        is_missing_clauses: list[SqlExpression] = [IS_NULL(column_name)]
        if isinstance(self.missing_values, list):
            literal_values = [LITERAL(value) for value in self.missing_values]
            is_missing_clauses.append(IN(column_name, literal_values))
        if isinstance(self.missing_regex_sql, str):
            is_missing_clauses.append(REGEX_LIKE(column_name, self.missing_regex_sql))
        return OR(is_missing_clauses)

    def get_sum_missing_count_expr(self, column_name: str) -> SqlExpression:
        missing_count_condition: SqlExpression = self.get_missing_count_condition(column_name)
        return SUM(CASE_WHEN(missing_count_condition, LITERAL(1), LITERAL(0)))

    def get_invalid_count_condition(self, column_name: str) -> SqlExpression:
        invalid_clauses: list[SqlExpression] = []
        if isinstance(self.valid_values, list):
            literal_values = [LITERAL(value) for value in self.valid_values]
            invalid_clauses.append(NOT(IN(column_name, literal_values)))
        if isinstance(self.valid_regex_sql, str):
            invalid_clauses.append(NOT(REGEX_LIKE(column_name, self.valid_regex_sql)))
        if isinstance(self.valid_format_regex, str):
            invalid_clauses.append(NOT(REGEX_LIKE(column_name, self.valid_format_regex)))
        if isinstance(self.valid_min, Number) or isinstance(self.valid_min, str):
            invalid_clauses.append(LT(column_name, LITERAL(self.valid_min)))
        if isinstance(self.valid_max, Number) or isinstance(self.valid_max, str):
            invalid_clauses.append(GT(column_name, LITERAL(self.valid_max)))
        if isinstance(self.invalid_regex_sql, str):
            invalid_clauses.append(REGEX_LIKE(column_name, self.invalid_regex_sql))
        if isinstance(self.invalid_format_regex, str):
            invalid_clauses.append(REGEX_LIKE(column_name, self.invalid_format_regex))
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

        check_has_missing: bool = self._has_missing_configurations()
        self.missing_values = self.missing_values if check_has_missing else column_defaults.missing_values
        self.missing_regex_sql = self.missing_regex_sql if check_has_missing else column_defaults.missing_regex_sql

        check_has_validity: bool = self._has_validity_configurations()
        self.invalid_values = self.invalid_values if check_has_validity else column_defaults.invalid_values
        self.invalid_format = self.invalid_format if check_has_validity else column_defaults.invalid_format
        self.invalid_format_regex = self.invalid_format if check_has_validity else column_defaults.invalid_format_regex
        self.invalid_regex_sql = self.invalid_regex_sql if check_has_validity else column_defaults.invalid_regex_sql
        self.valid_values = self.valid_values if check_has_validity else column_defaults.valid_values
        self.valid_format = self.valid_format if check_has_validity else column_defaults.valid_format
        self.valid_format_regex = self.valid_format if check_has_validity else column_defaults.valid_format_regex
        self.valid_regex_sql = self.valid_regex_sql if check_has_validity else column_defaults.valid_regex_sql
        self.valid_min = self.valid_min if check_has_validity else column_defaults.valid_min
        self.valid_max = self.valid_max if check_has_validity else column_defaults.valid_max
        self.valid_length = self.valid_length if check_has_validity else column_defaults.valid_length
        self.valid_min_length = self.valid_min_length if check_has_validity else column_defaults.valid_min_length
        self.valid_max_length = self.valid_max_length if check_has_validity else column_defaults.valid_max_length
        self.valid_reference_data = self.valid_reference_data if check_has_validity else column_defaults.valid_reference_data

    def _has_missing_configurations(self) -> bool:
        return (self.missing_values is not None
                or self.missing_regex_sql is not None)

    def _has_validity_configurations(self) -> bool:
        return (self.invalid_values is not None
                or self.invalid_format is not None
                or self.invalid_regex_sql  is not None
                or self.valid_values is not None
                or self.valid_format is not None
                or self.valid_regex_sql is not None
                or self.valid_min is not None
                or self.valid_max is not None
                or self.valid_length is not None
                or self.valid_min_length is not None
                or self.valid_max_length is not None
                or self.valid_reference_data is not None)

    def has_reference_data(self) -> bool:
        return isinstance(self.valid_reference_data, ValidReferenceData)


class MetricsResolver:
    def __init__(self):
        self.metrics: list[MetricImpl] = []

    def resolve_metric(self, metric_impl: MetricImpl) -> MetricImpl:
        existing_metric_impl: MetricImpl | None = next((m for m in self.metrics if m == metric_impl), None)
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
    def create(cls, check_yaml: ThresholdCheckYaml, default_threshold: ThresholdImpl | None = None) -> ThresholdImpl | None:
        total_config_count: int = cls.__config_count([
            check_yaml.must_be_greater_than, check_yaml.must_be_greater_than_or_equal, check_yaml.must_be_less_than,
            check_yaml.must_be_less_than_or_equal, check_yaml.must_be, check_yaml.must_not_be,
            check_yaml.must_be_between, check_yaml.must_be_not_between])

        if total_config_count == 0:
            if default_threshold:
                return default_threshold
            check_yaml.logs.error("Threshold required, but not specified")
            return None

        if total_config_count == 1 and cls.__config_count([
            check_yaml.must_be_greater_than, check_yaml.must_be_greater_than_or_equal, check_yaml.must_be_less_than,
            check_yaml.must_be_less_than_or_equal, check_yaml.must_be, check_yaml.must_not_be]) == 1:
            return ThresholdImpl(
                type=ThresholdType.SINGLE_COMPARATOR,
                must_be_greater_than=check_yaml.must_be_greater_than,
                must_be_greater_than_or_equal=check_yaml.must_be_greater_than_or_equal,
                must_be_less_than=check_yaml.must_be_less_than,
                must_be_less_than_or_equal=check_yaml.must_be_less_than_or_equal,
                must_be=check_yaml.must_be,
                must_not_be=check_yaml.must_not_be,
            )

        elif total_config_count == 1 and isinstance(check_yaml.must_be_between, RangeYaml):
            if (isinstance(check_yaml.must_be_between.lower_bound, Number)
               and isinstance(check_yaml.must_be_between.upper_bound, Number)):
                if check_yaml.must_be_between.lower_bound < check_yaml.must_be_between.upper_bound:
                    return ThresholdImpl(
                        type=ThresholdType.INNER_RANGE,
                        must_be_greater_than_or_equal=check_yaml.must_be_between.lower_bound,
                        must_be_less_than_or_equal=check_yaml.must_be_between.upper_bound,
                    )
                else:
                    check_yaml.logs.error("Threshold must_be_between range: "
                                          "first value must be less than the second value")
                    return None

        elif total_config_count == 1 and isinstance(check_yaml.must_be_not_between, RangeYaml):
            if (isinstance(check_yaml.must_be_not_between.lower_bound, Number)
               and isinstance(check_yaml.must_be_not_between.upper_bound, Number)):
                if check_yaml.must_be_between.lower_bound < check_yaml.must_be_between.upper_bound:
                    return ThresholdImpl(
                        type=ThresholdType.OUTER_RANGE,
                        must_be_greater_than_or_equal=check_yaml.must_be_not_between.upper_bound,
                        must_be_less_than_or_equal=check_yaml.must_be_not_between.lower_bound,
                    )
                else:
                    check_yaml.logs.error("Threshold must_be_not_between range: "
                                          "first value must be less than the second value")
                    return None
        else:
            lower_bound_count = cls.__config_count([check_yaml.must_be_greater_than, check_yaml.must_be_greater_than_or_equal])
            upper_bound_count = cls.__config_count([check_yaml.must_be_less_than, check_yaml.must_be_less_than_or_equal])
            if lower_bound_count == 1 and upper_bound_count == 1:
                lower_bound = check_yaml.must_be_greater_than if check_yaml.must_be_greater_than is not None else check_yaml.must_be_greater_than_or_equal
                upper_bound = check_yaml.must_be_less_than if check_yaml.must_be_less_than is not None else check_yaml.must_be_less_than_or_equal
                if lower_bound < upper_bound:
                    return ThresholdImpl(
                        type=ThresholdType.INNER_RANGE,
                        must_be_greater_than=check_yaml.must_be_greater_than,
                        must_be_greater_than_or_equal=check_yaml.must_be_greater_than_or_equal,
                        must_be_less_than=check_yaml.must_be_less_than,
                        must_be_less_than_or_equal=check_yaml.must_be_less_than_or_equal,
                    )
                else:
                    return ThresholdImpl(
                        type=ThresholdType.OUTER_RANGE,
                        must_be_greater_than=check_yaml.must_be_greater_than,
                        must_be_greater_than_or_equal=check_yaml.must_be_greater_than_or_equal,
                        must_be_less_than=check_yaml.must_be_less_than,
                        must_be_less_than_or_equal=check_yaml.must_be_less_than_or_equal,
                    )

    @classmethod
    def __config_count(cls, members: list[any]) -> int:
        return sum([
            0 if v is None else 1
            for v in members])

    def __init__(self,
                 type: ThresholdType,
                 must_be_greater_than: Number | None = None,
                 must_be_greater_than_or_equal: Number | None = None,
                 must_be_less_than: Number | None = None,
                 must_be_less_than_or_equal: Number | None = None,
                 must_be: Number | None = None,
                 must_not_be: Number | None = None
                 ):
        self.type: ThresholdType = type
        self.must_be_greater_than: Number | None = must_be_greater_than
        self.must_be_greater_than_or_equal: Number | None = must_be_greater_than_or_equal
        self.must_be_less_than: Number | None = must_be_less_than
        self.must_be_less_than_or_equal: Number | None = must_be_less_than_or_equal
        self.must_be: Number | None = must_be
        self.must_not_be: Number | None = must_not_be

    def to_threshold_info(self) -> Threshold:
        if self.must_be is None and self.must_not_be is None:
            return Threshold(
                must_be_greater_than=self.must_be_greater_than,
                must_be_greater_than_or_equal=self.must_be_greater_than_or_equal,
                must_be_less_than=self.must_be_less_than,
                must_be_less_than_or_equal=self.must_be_less_than_or_equal
            )
        elif self.must_be is not None:
            return Threshold(
                must_be_greater_than_or_equal=self.must_be,
                must_be_less_than_or_equal=self.must_be
            )
        elif self.must_not_be is not None:
            return Threshold(
                must_be_greater_than=self.must_not_be,
                must_be_less_than=self.must_not_be
            )

    @classmethod
    def get_metric_name(cls, metric_name: str, column_impl: ColumnImpl | None) -> str:
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
            gt_bound: str = (str(self.must_be_greater_than) if isinstance(self.must_be_greater_than, Number)
                                else str(self.must_be_greater_than_or_equal))
            lt_comparator: str = " < " if isinstance(self.must_be_less_than, Number) else " <= "
            lt_bound: str = (str(self.must_be_less_than) if isinstance(self.must_be_less_than, Number)
                                else str(self.must_be_less_than_or_equal))
            if self.type == ThresholdType.INNER_RANGE:
                return f"{gt_bound}{gt_comparator}{metric_name}{lt_comparator}{lt_bound}"
            else:
                return f"{metric_name}{lt_comparator}{lt_bound} or {gt_bound}{gt_comparator}{metric_name}"

    def passes(self, value: Number) -> bool:
        return ((self.must_be_greater_than is None or value > self.must_be_greater_than)
           and (self.must_be_greater_than_or_equal is None or value >= self.must_be_greater_than_or_equal)
           and (self.must_be_less_than is None or value < self.must_be_less_than)
           and (self.must_be_less_than_or_equal is None or value <= self.must_be_less_than_or_equal)
           and (not (isinstance(self.must_be, Number) or isinstance(self.must_be, str)) or value == self.must_be)
           and (not (isinstance(self.must_not_be, Number) or isinstance(self.must_not_be, str)) or value != self.must_not_be)
        )


class CheckParser(ABC):

    @abstractmethod
    def get_check_type_names(self) -> list[str]:
        pass

    @abstractmethod
    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl | None,
        check_yaml: CheckYaml,
        metrics_resolver: MetricsResolver,
    ) -> CheckImpl | None:
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
        metrics_resolver: MetricsResolver,
        column_impl: ColumnImpl | None = None,
    ) -> CheckImpl | None:
        if isinstance(check_yaml.type, str):
            check_parser: CheckParser | None = cls.check_parsers.get(check_yaml.type)
            if check_parser:
                return check_parser.parse_check(
                    contract_impl=contract_impl,
                    column_impl=column_impl,
                    check_yaml=check_yaml,
                    metrics_resolver=metrics_resolver,
                )
            else:
                contract_impl.logs.error(f"Unknown check type '{check_yaml.type}'")

    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl | None,
        check_yaml: CheckYaml,
    ):
        self.logs: Logs = contract_impl.logs

        self.contract_impl: ContractImpl = contract_impl
        self.check_yaml: CheckYaml = check_yaml
        self.column_impl: ColumnImpl | None = column_impl
        self.type: str = check_yaml.type
        self.name: str | None = None
        self.identity: str = self._build_identity(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_type=check_yaml.type,
            qualifier=check_yaml.qualifier
        )

        self.threshold: ThresholdImpl | None = None
        self.metrics: list[MetricImpl] = []
        self.queries: list[Query] = []
        self.skip: bool = False

    def _resolve_metric(self, metric_impl: MetricImpl) -> MetricImpl:
        resolved_metric_impl: MetricImpl = self.contract_impl.metrics_resolver.resolve_metric(metric_impl)
        self.metrics.append(resolved_metric_impl)
        return resolved_metric_impl

    @abstractmethod
    def evaluate(self, measurement_values: MeasurementValues, contract_info: Contract) -> CheckResult:
        pass

    def _build_check_info(self) -> Check:
        return Check(
            type=self.type,
            name=self.name,
            identity=self.identity,
            definition=self._build_definition(),
            column_name=self.column_impl.column_yaml.name if self.column_impl else None,
            contract_file_line=self.check_yaml.check_yaml_object.location.line,
            contract_file_column=self.check_yaml.check_yaml_object.location.column,
            threshold=self._build_threshold()
        )

    def _build_identity(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl | None,
        check_type: str,
        qualifier: str | None
    ) -> str:
        identity_hash_builder: ConsistentHashBuilder = ConsistentHashBuilder(8)
        identity_hash_builder.add_property("fp", contract_impl.contract_yaml.contract_yaml_file_content.yaml_file_path)
        identity_hash_builder.add_property("c", column_impl.column_yaml.name if column_impl else None)
        identity_hash_builder.add_property("t", check_type)
        identity_hash_builder.add_property("q", qualifier)
        return identity_hash_builder.get_hash()

    def _build_definition(self) -> str:
        text_stream = StringIO()
        yaml = YAML()
        yaml.dump(self.check_yaml.check_yaml_object.to_dict(), text_stream)
        text_stream.seek(0)
        return text_stream.read()

    def _build_threshold(self) -> Threshold | None:
        return self.threshold.to_threshold_info() if self.threshold else None


class MissingAndValidityCheckImpl(CheckImpl):

    def __init__(self, contract_impl: ContractImpl, column_impl: ColumnImpl | None, check_yaml: MissingAncValidityCheckYaml):
        super().__init__(contract_impl, column_impl, check_yaml)
        self.missing_and_validity: MissingAndValidity = MissingAndValidity(
            missing_and_validity_yaml=check_yaml,
            data_source=contract_impl.data_source
        )
        self.missing_and_validity.apply_column_defaults(column_impl)


class MetricImpl:

    def __init__(
        self,
        contract_impl: ContractImpl,
        metric_type: str,
        column_impl: ColumnImpl | None = None,
    ):
        # TODO id of a metric will have to be extended to include check configuration parameters that
        #      influence the metric identity
        self.id: str = self.create_metric_id(contract_impl, metric_type, column_impl)
        self.contract_impl: ContractImpl = contract_impl
        self.column_impl: ColumnImpl | None = column_impl
        self.type: str = metric_type

    @classmethod
    def create_metric_id(
        cls,
        contract_impl: ContractImpl,
        metric_type: str,
        column_impl: ColumnImpl | None = None,
    ):
        id_parts: list[str] = [contract_impl.data_source.name]
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
        column_impl: ColumnImpl | None = None,
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
    def __init__(
        self,
        metric_type: str,
        fraction_metric_impl: MetricImpl,
        total_metric_impl: MetricImpl
    ):
        super().__init__(
            contract_impl=fraction_metric_impl.contract_impl,
            column_impl=fraction_metric_impl.column_impl,
            metric_type=metric_type
        )
        self.fraction_metric_impl: MetricImpl = fraction_metric_impl
        self.total_metric_impl: MetricImpl = total_metric_impl

    def create_derived_measurement(self, measurement_values: MeasurementValues) -> Measurement:
        fraction: Number = measurement_values.get_value(self.fraction_metric_impl)
        total: Number = measurement_values.get_value(self.total_metric_impl)
        if isinstance(fraction, Number) and isinstance(total, Number):
            value: float = (fraction * 100 / total) if total != 0 else 0
            return Measurement(
                metric_id=self.id,
                value=value,
                metric_name=self.type
            )


class Query(ABC):

    def __init__(
        self,
        data_source: DataSource,
        metrics: list[MetricImpl],
        sql: str | None = None
    ):
        self.data_source: DataSource = data_source
        self.metrics: list[MetricImpl] = metrics
        self.sql: str | None = sql

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
        filter_condition: str | None,
        data_source: DataSource
    ):
        super().__init__(data_source=data_source, metrics=[])
        self.dataset_prefix: list[str] = dataset_prefix
        self.dataset_name: str = dataset_name
        self.filter_condition: str = filter_condition
        self.aggregation_metrics: list[AggregationMetricImpl] = []
        self.data_source: DataSource = data_source
        self.query_size: int = len(self.build_sql())

    def can_accept(self, aggregation_metric_impl: AggregationMetricImpl) -> bool:
        sql_expression: SqlExpression = aggregation_metric_impl.sql_expression()
        sql_expression_str: str = self.data_source.sql_dialect.build_expression_sql(sql_expression)
        return self.query_size + len(sql_expression_str) < self.data_source.get_max_aggregation_query_length()

    def append_aggregation_metric(self, aggregation_metric_impl: AggregationMetricImpl) -> None:
        self.aggregation_metrics.append(aggregation_metric_impl)

    def build_sql(self) -> str:
        field_expressions: list[SqlExpression] = self.build_field_expressions()
        select = [
            SELECT(field_expressions),
            FROM(self.dataset_name, self.dataset_prefix)
        ]
        if self.filter_condition:
            select.append(WHERE(SqlExpressionStr(self.filter_condition)))
        self.sql = self.data_source.sql_dialect.build_select_sql(select)
        return self.sql

    def build_field_expressions(self) -> list[SqlExpression]:
        if len(self.aggregation_metrics) == 0:
            # This is to get the initial query length in the constructor
            return [COUNT(STAR())]
        return [
            aggregation_metric.sql_expression()
            for aggregation_metric in self.aggregation_metrics
        ]

    def execute(self) -> list[Measurement]:
        measurements: list[Measurement] = []
        sql = self.build_sql()
        query_result: QueryResult = self.data_source.execute_query(sql)
        row: tuple = query_result.rows[0]
        for i in range(0, len(self.aggregation_metrics)):
            aggregation_metric_impl: AggregationMetricImpl = self.aggregation_metrics[i]
            measurement_value = aggregation_metric_impl.convert_db_value(row[i])
            measurements.append(Measurement(
                metric_id=aggregation_metric_impl.id,
                value=measurement_value,
                metric_name=aggregation_metric_impl.get_short_description()
            ))
        return measurements
