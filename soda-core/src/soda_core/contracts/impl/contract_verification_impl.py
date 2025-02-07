from __future__ import annotations

import os
from abc import abstractmethod, ABC
from datetime import timezone
from enum import Enum
from symbol import varargslist

from cfgv import check_string

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_parser import DataSourceParser
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logs import Logs
from soda_core.common.sql_dialect import *
from soda_core.common.yaml import YamlSource, VariableResolver
from soda_core.contracts.contract_verification import ContractVerificationResult, ContractResult, \
    CheckResult, Measurement
from soda_core.contracts.impl.contract_yaml import ContractYaml, CheckYaml, ColumnYaml, RangeYaml, \
    MissingAndValidityYaml, ValidReferenceDataYaml, MissingAncValidityCheckYaml, ThresholdCheckYaml
from soda_core.tests.helpers.consistent_hash_builder import ConsistentHashBuilder


class DataSourceContracts:

    def __init__(self, data_source: DataSource):
        self.data_source: DataSource = data_source
        self.contracts: list[Contract] = []

    def add_contract(self, contract: Contract) -> None:
        self.contracts.append(contract)


class ContractVerificationImpl:

    def __init__(
            self,
            default_data_source: DataSource,
            contract_yaml_sources: list[YamlSource],
            variables: dict[str, str],
            logs: Logs = Logs(),
            soda_cloud: 'SodaCloud' | None = None
    ):
        self.logs: Logs = logs
        self.default_data_source_contracts: DataSourceContracts | None = None
        self.data_sources_contracts: list[DataSourceContracts] = []
        self.soda_cloud: 'SodaCloud' | None = soda_cloud

        if default_data_source:
            self.default_data_source_contracts = DataSourceContracts(data_source=default_data_source)
            self.data_sources_contracts.append(self.default_data_source_contracts)

        for contract_yaml_source in contract_yaml_sources:
            contract_yaml: ContractYaml = ContractYaml.parse(contract_yaml_source=contract_yaml_source, variables=variables, logs=logs)
            if contract_yaml:
                data_source_contracts: DataSourceContracts = self.resolve_data_source_contracts(contract_yaml)
                if data_source_contracts:
                    data_source: DataSource = data_source_contracts.data_source
                    contract: Contract = Contract(
                        contract_yaml=contract_yaml, data_source=data_source, variables=variables, logs=self.logs
                    )
                    data_source_contracts.contracts.append(contract)

    def resolve_data_source_contracts(self, contract_yaml: ContractYaml) -> DataSourceContracts | None:
        data_source_file_is_configured: bool = isinstance(contract_yaml.data_source_file, str)
        if data_source_file_is_configured:
            contract_yaml_file_path: str = contract_yaml.contract_yaml_file_content.yaml_file_path
            contract_file_exists: bool = isinstance(contract_yaml_file_path, str) and os.path.exists(
                contract_yaml_file_path)
            if not contract_file_exists:
                self.logs.error(
                    "'data_source_file' is configured, but that can't be used with contract yaml string or yaml dict."
                )
            else:
                real_contract_path: str = os.path.realpath(contract_yaml_file_path)
                contract_dir: str = os.path.dirname(real_contract_path)

                data_source_relative_path: str = contract_yaml.data_source_file
                data_source_path: str = os.path.join(contract_dir, data_source_relative_path)
                real_data_source_path: str = os.path.realpath(data_source_path)

                data_source_contracts: DataSourceContracts = self.find_existing_data_source_contracts(real_data_source_path)

                if not data_source_contracts:
                    data_source_yaml_source: YamlSource = YamlSource.from_file_path(yaml_file_path=real_data_source_path)
                    data_source_parser: DataSourceParser = DataSourceParser(data_source_yaml_source, logs=self.logs)
                    data_source = data_source_parser.parse()
                    data_source_contracts = DataSourceContracts(data_source=data_source)

                return data_source_contracts
        if self.default_data_source_contracts:
            return self.default_data_source_contracts
        else:
            self.logs.error(
                f"'data_source_file' is required. "
                f"No default data source was configured in the contract verification builder."
            )

    def find_existing_data_source_contracts(self, real_data_source_path: str) -> DataSourceContracts | None:
        for data_source_contracts in self.data_sources_contracts:
            if real_data_source_path == data_source_contracts.data_source.data_source_yaml_file_content.yaml_file_path:
                return data_source_contracts
        return None

    def execute(self) -> ContractVerificationResult:
        contract_results: list[ContractResult] = []

        for data_source_contracts in self.data_sources_contracts:
            if isinstance(data_source_contracts.data_source, DataSource) and len(data_source_contracts.contracts) > 0:
                data_source_contract_results: list[ContractResult] = self.verify_data_source_contracts(
                    data_source_contracts
                )
                contract_results.extend(data_source_contract_results)
            else:
                self.logs.error("No data source configured")

        return ContractVerificationResult(
            logs=self.logs,
            contract_results=contract_results
        )

    def verify_data_source_contracts(self, data_source_contracts: DataSourceContracts) -> list[ContractResult]:
        contract_results: list[ContractResult] = []
        data_source = data_source_contracts.data_source
        open_close: bool = not data_source.has_open_connection()
        if open_close:
            data_source.open_connection()
        try:
            if len(data_source_contracts.contracts) == 0:
                self.logs.error("No contracts specified")
            else:
                for contract in data_source_contracts.contracts:
                    contract_result: ContractResult = contract.verify()
                    contract_results.append(contract_result)
                    if self.soda_cloud:
                        self.soda_cloud.send_contract_result(contract_result)
        finally:
            if open_close:
                data_source.close_connection()
        return contract_results


class Contract:

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

        data_source_location: dict[str, str] | None = (
            contract_yaml.dataset_locations.get(data_source.get_data_source_type_name())
            if contract_yaml.dataset_locations else None
        )
        self.dataset_prefix: list[str] | None = data_source.build_dataset_prefix(data_source_location)
        self.dataset_name: str | None = contract_yaml.dataset_name if contract_yaml else None

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
        self.columns: list[Column] = self._parse_columns(
            contract_yaml=contract_yaml,
        )

        self.checks: list[Check] = self._parse_checks(contract_yaml)

        self.all_checks: list[Check] = list(self.checks)
        for column in self.columns:
            self.all_checks.extend(column.checks)

        self._verify_duplicate_identities(self.all_checks, self.logs)

        self.metrics: list[Metric] = self.metrics_resolver.get_resolved_metrics()
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
    ) -> list[Check]:
        checks: list[Check] = []
        if contract_yaml.checks:
            for check_yaml in contract_yaml.checks:
                if check_yaml:
                    check = Check.parse_check(
                        contract=self,
                        check_yaml=check_yaml,
                        metrics_resolver=self.metrics_resolver,
                    )
                    checks.append(check)
        return checks

    def _build_queries(self) -> list[Query]:
        queries: list[Query] = []
        aggregation_metrics: list[AggregationMetric] = []

        for check in self.all_checks:
            queries.extend(check.queries)

        for metric in self.metrics:
            if isinstance(metric, AggregationMetric):
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

    def _parse_columns(self, contract_yaml: ContractYaml) -> list[Column]:
        columns: list[Column] = []
        if contract_yaml.columns:
            for column_yaml in contract_yaml.columns:
                column = Column(
                    contract=self,
                    column_yaml=column_yaml,
                    metrics_resolver=self.metrics_resolver
                )
                columns.append(column)
        return columns

    def verify(self) -> ContractResult:
        measurements: list[Measurement] = []
        # Executing the queries will set the value of the metrics linked to queries
        for query in self.queries:
            query_measurements: list[Measurement] = query.execute()
            measurements.extend(query_measurements)

        # Triggering the derived metrics to initialize their value based on their dependencies
        derived_metrics: list[DerivedPercentageMetric] = [
            derived_metric for derived_metric in self.metrics
            if isinstance(derived_metric, DerivedPercentageMetric)
        ]
        measurement_values: MeasurementValues = MeasurementValues(measurements)
        for derived_metric in derived_metrics:
            derived_measurement: Measurement = derived_metric.create_derived_measurement(measurement_values)
            measurements.append(derived_measurement)

        # Evaluate the checks
        measurement_values = MeasurementValues(measurements)
        check_results: list[CheckResult] = []
        for check in self.all_checks:
            check_result: CheckResult = check.evaluate(measurement_values=measurement_values)
            check_results.append(check_result)

        return ContractResult(
            data_timestamp=self.data_timestamp,
            started_timestamp=self.started_timestamp,
            ended_timestamp=datetime.now(tz=timezone.utc),
            data_source_name=self.data_source.name,
            soda_qualified_dataset_name=self.soda_qualified_dataset_name,
            sql_qualified_dataset_name=self.sql_qualified_dataset_name,
            measurements=measurements,
            check_results=check_results,
            logs=self.logs
        )

    @classmethod
    def _verify_duplicate_identities(cls, all_checks: list[Check], logs: Logs):
        checks_by_identity: dict[str, Check] = {}
        for check in all_checks:
            existing_check: Check | None = checks_by_identity.get(check.identity)
            if existing_check:
                # TODO distill better diagnostic error message: which check in which column on which lines in the file
                logs.error(f"Duplicate identity ({check.identity})")
            checks_by_identity[check.identity] = check


class MeasurementValues:
    def __init__(self, measurements: list[Measurement]):
        self.measurement_values_by_metric_id: dict[str, any] = {
            measurement.metric_id: measurement.value
            for measurement in measurements
        }

    def get_value(self, metric: Metric) -> any:
        return self.measurement_values_by_metric_id.get(metric.id)


class Column:
    def __init__(self, contract: Contract, column_yaml: ColumnYaml, metrics_resolver: MetricsResolver):
        self.column_yaml = column_yaml
        self.missing_and_validity: MissingAndValidity = MissingAndValidity(
            missing_and_validity_yaml=column_yaml,
            data_source=contract.data_source
        )
        self.checks: list[Check] = []
        if column_yaml.checks:
            for check_yaml in column_yaml.checks:
                if check_yaml:
                    check = Check.parse_check(
                        contract=contract,
                        column=self,
                        check_yaml=check_yaml,
                        metrics_resolver=metrics_resolver,
                    )
                    self.checks.append(check)


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

    def apply_column_defaults(self, column: Column) -> None:
        if not column:
            return
        column_defaults: MissingAndValidity = column.missing_and_validity
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
        self.metrics: list[Metric] = []

    def resolve_metric(self, metric: Metric) -> Metric:
        existing_metric: Metric | None = next((m for m in self.metrics if m == metric), None)
        if existing_metric:
            return existing_metric
        else:
            self.metrics.append(metric)
            return metric

    def get_resolved_metrics(self) -> list[Metric]:
        return self.metrics


class ThresholdType(Enum):
    SINGLE_COMPARATOR = "single_comparator"
    INNER_RANGE = "inner_range"
    OUTER_RANGE = "outer_range"


class Threshold:

    @classmethod
    def create(cls, check_yaml: ThresholdCheckYaml, default_threshold: Threshold | None = None) -> Threshold | None:
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
            return Threshold(
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
                    return Threshold(
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
                    return Threshold(
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
                    return Threshold(
                        type=ThresholdType.INNER_RANGE,
                        must_be_greater_than=check_yaml.must_be_greater_than,
                        must_be_greater_than_or_equal=check_yaml.must_be_greater_than_or_equal,
                        must_be_less_than=check_yaml.must_be_less_than,
                        must_be_less_than_or_equal=check_yaml.must_be_less_than_or_equal,
                    )
                else:
                    return Threshold(
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
        contract: Contract,
        column: Column | None,
        check_yaml: CheckYaml,
        metrics_resolver: MetricsResolver,
    ) -> Check | None:
        pass


class Check:
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
        contract: Contract,
        check_yaml: CheckYaml,
        metrics_resolver: MetricsResolver,
        column: Column | None = None,
    ) -> Check | None:
        if isinstance(check_yaml.type, str):
            check_parser: CheckParser | None = cls.check_parsers.get(check_yaml.type)
            if check_parser:
                return check_parser.parse_check(
                    contract=contract,
                    column=column,
                    check_yaml=check_yaml,
                    metrics_resolver=metrics_resolver,
                )
            else:
                contract.logs.error(f"Unknown check type '{check_yaml.type}'")

    def __init__(
        self,
        contract: Contract,
        column: Column | None,
        check_yaml: CheckYaml,
    ):
        self.logs: Logs = contract.logs

        self.contract: Contract = contract
        self.check_yaml: CheckYaml = check_yaml
        self.column: Column | None = column
        self.type: str = check_yaml.type
        self.name: str | None = None
        self.identity: str = self._build_identity(
            contract=contract,
            column=column,
            check_type=check_yaml.type,
            qualifier=check_yaml.qualifier
        )

        self.threshold: Threshold | None = None
        self.metrics: list[Metric] = []
        self.queries: list[Query] = []
        self.skip: bool = False

    def _resolve_metric(self, metric: Metric) -> Metric:
        resolved_metric: Metric = self.contract.metrics_resolver.resolve_metric(metric)
        self.metrics.append(resolved_metric)
        return resolved_metric

    @abstractmethod
    def evaluate(self, measurement_values: MeasurementValues) -> CheckResult:
        pass

    def _build_identity(
        self,
        contract: Contract,
        column: Column | None,
        check_type: str,
        qualifier: str | None
    ) -> str:
        identity_hash_builder: ConsistentHashBuilder = ConsistentHashBuilder(8)
        identity_hash_builder.add_property("fp", contract.contract_yaml.contract_yaml_file_content.yaml_file_path)
        identity_hash_builder.add_property("c", column.column_yaml.name if column else None)
        identity_hash_builder.add_property("t", check_type)
        identity_hash_builder.add_property("q", qualifier)
        return identity_hash_builder.get_hash()


class MissingAndValidityCheck(Check):

    def __init__(self, contract: Contract, column: Column | None, check_yaml: MissingAncValidityCheckYaml):
        super().__init__(contract, column, check_yaml)
        self.missing_and_validity: MissingAndValidity = MissingAndValidity(
            missing_and_validity_yaml=check_yaml,
            data_source=contract.data_source
        )
        self.missing_and_validity.apply_column_defaults(column)


class Metric:

    def __init__(
        self,
        contract: Contract,
        metric_type: str,
        column: Column | None = None,
    ):
        # TODO id of a metric will have to be extended to include check configuration parameters that
        #      influence the metric identity
        self.id: str = self.create_metric_id(contract, metric_type, column)
        self.contract: Contract = contract
        self.column: Column | None = column
        self.type: str = metric_type

    @classmethod
    def create_metric_id(
        cls,
        contract: Contract,
        metric_type: str,
        column: Column | None = None,
    ):
        id_parts: list[str] = [contract.data_source.name]
        if contract.dataset_prefix:
            id_parts.extend(contract.dataset_prefix)
        if column:
            id_parts.append(column.column_yaml.name)
        id_parts.append(metric_type)
        return "/" + "/".join(id_parts)

    def __eq__(self, other):
        if type(other) != type(self):
            return False
        return self.id == other.id


class AggregationMetric(Metric):

    def __init__(
        self,
        contract: Contract,
        metric_type: str,
        column: Column | None = None,
    ):
        super().__init__(
            contract=contract,
            column=column,
            metric_type=metric_type,
        )

    @abstractmethod
    def sql_expression(self) -> SqlExpression:
        pass

    def convert_db_value(self, value: any) -> any:
        return value

    def get_short_description(self) -> str:
        return self.type



class DerivedPercentageMetric(Metric):
    def __init__(
        self,
        metric_type: str,
        fraction_metric: Metric,
        total_metric: Metric
    ):
        super().__init__(
            contract=fraction_metric.contract,
            column=fraction_metric.column,
            metric_type=metric_type
        )
        self.fraction_metric: Metric = fraction_metric
        self.total_metric: Metric = total_metric

    def create_derived_measurement(self, measurement_values: MeasurementValues) -> Measurement:
        fraction: Number = measurement_values.get_value(self.fraction_metric)
        total: Number = measurement_values.get_value(self.total_metric)
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
        metrics: list[Metric],
        sql: str | None = None
    ):
        self.data_source: DataSource = data_source
        self.metrics: list[Metric] = metrics
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
        self.aggregation_metrics: list[AggregationMetric] = []
        self.data_source: DataSource = data_source
        self.query_size: int = len(self.build_sql())

    def can_accept(self, aggregation_metric: AggregationMetric) -> bool:
        sql_expression: SqlExpression = aggregation_metric.sql_expression()
        sql_expression_str: str = self.data_source.sql_dialect.build_expression_sql(sql_expression)
        return self.query_size + len(sql_expression_str) < self.data_source.get_max_aggregation_query_length()

    def append_aggregation_metric(self, aggregation_metric: AggregationMetric) -> None:
        self.aggregation_metrics.append(aggregation_metric)

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
            aggregation_metric: AggregationMetric = self.aggregation_metrics[i]
            measurement_value = aggregation_metric.convert_db_value(row[i])
            measurements.append(Measurement(
                metric_id=aggregation_metric.id,
                value=measurement_value,
                metric_name=aggregation_metric.get_short_description()
            ))
        return measurements
