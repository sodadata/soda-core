from __future__ import annotations

import os
from abc import abstractmethod, ABC
from enum import Enum

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_parser import DataSourceParser
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logs import Logs
from soda_core.common.sql_dialect import *
from soda_core.common.yaml import YamlSource
from soda_core.contracts.contract_verification import ContractVerificationResult, ContractResult, \
    CheckResult
from soda_core.contracts.impl.contract_yaml import ContractYaml, CheckYaml, ColumnYaml, RangeYaml


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
            logs: Logs = Logs()
    ):
        self.logs: Logs = logs
        self.default_data_source_contracts: DataSourceContracts | None = None
        self.data_sources_contracts: list[DataSourceContracts] = []
        if default_data_source:
            self.default_data_source_contracts = DataSourceContracts(data_source=default_data_source)
            self.data_sources_contracts.append(self.default_data_source_contracts)

        for contract_yaml_source in contract_yaml_sources:
            contract_yaml: ContractYaml = ContractYaml.parse(contract_yaml_source=contract_yaml_source, variables=variables, logs=logs)
            if contract_yaml:
                data_source_contracts: DataSourceContracts = self.resolve_data_source_contracts(contract_yaml)
                if data_source_contracts:
                    data_source: DataSource = data_source_contracts.data_source
                    contract: Contract = Contract(contract_yaml=contract_yaml, data_source=data_source, logs=self.logs)
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

    # def _parse_soda_cloud(self, contract_verification_builder: ContractVerificationBuilder) -> None:
    #     soda_cloud_file: YamlSource | None = contract_verification_builder.soda_cloud_file
    #     if isinstance(soda_cloud_file, YamlSource) and soda_cloud_file.exists():
    #         soda_cloud_file.parse(contract_verification_builder.variables)
    #         self.soda_cloud = SodaCloud(soda_cloud_file)
    #
    # def _parse_plugins(self, contract_verification_builder) -> None:
    #     plugin_files_by_type: dict[str, list[YamlSource]] = {}
    #     for plugin_file in contract_verification_builder.plugin_files:
    #         plugin_file.parse(self.variables)
    #         if isinstance(plugin_file.dict, dict):
    #             plugin: str | None = plugin_file.dict.get("plugin")
    #             if isinstance(plugin, str):
    #                 plugin_files_by_type.setdefault(plugin, []).append(plugin_file)
    #             else:
    #                 self.logs.error(f"Key plugin is required in plugin YAML files")
    #         else:
    #             self.logs.error(f"Could not read plugin YAML file {plugin_file.file_path}")
    #     for plugin_name, plugin_yaml_files in plugin_files_by_type.items():
    #         for plugin_yaml_file in plugin_yaml_files:
    #             plugin_yaml_file.parse(self.variables)
    #         plugin: Plugin = Plugin.create(plugin_name, plugin_yaml_files, self.logs)
    #         if isinstance(plugin, Plugin):
    #             self.plugins.append(plugin)
    #         else:
    #             self.logs.error(f"Could not load plugin {plugin_name}")

    def verify_data_source_contracts(self, data_source_contracts: DataSourceContracts) -> list[ContractResult]:
        contract_results: list[ContractResult] = []
        data_source = data_source_contracts.data_source
        open_close: bool = data_source.has_open_connection()
        if open_close:
            data_source.open_connection()
        try:
            if len(data_source_contracts.contracts) == 0:
                self.logs.error("No contracts specified")
            else:
                for contract in data_source_contracts.contracts:
                    contract_result: ContractResult = self.verify_contract(contract)
                    contract_results.append(contract_result)
        finally:
            if open_close:
                data_source.close_connection()
        return contract_results

    def verify_contract(self, contract: Contract) -> ContractResult:
        # Ensures that all contract->check->metrics that are equal are merged into the same Python object
        for query in contract.queries:
            query.execute()

        check_results: list[CheckResult] = []
        for check in contract.checks:
            check_result: CheckResult = check.evaluate()
            check_results.append(check_result)

        return ContractResult(
            contract_yaml=contract.contract_yaml,
            check_results=check_results,
            logs=self.logs
        )


# class CheckDependencyResolver:
#     def __init__(self, checks: list[Check]):
#         self.checks_not_evaluated: list[Check] = checks
#         self.checks_evaluating_batch: list[Check] = []
#         self.checks_evaluated: list[Check] = []
#
#     def next_dependency_batch(self) -> bool:
#         self.checks_evaluated.extend(self.checks_evaluating_batch)
#         self.checks_evaluating_batch = []
#         for check_not_evaluated in self.checks_not_evaluated:
#             if self.has_all_dependencies_available(check_not_evaluated):
#                 self.checks_not_evaluated.remove(check_not_evaluated)
#                 self.checks_evaluating_batch.append(check_not_evaluated)
#         return len(self.checks_evaluating_batch) > 0
#
#     def has_all_dependencies_available(self, check_not_evaluated: Check) -> bool:
#         return all(
#             check_dependency in self.checks_evaluated
#             for check_dependency in check_not_evaluated.check_dependencies
#         )
#
#     def get_next_dependency_batch(self) -> list[Check]:
#         return self.checks_evaluating_batch


class Contract:

    def __init__(self, contract_yaml: ContractYaml, data_source: DataSource, logs: Logs):
        self.logs: Logs = logs
        self.data_source: DataSource = data_source
        self.contract_yaml: ContractYaml = contract_yaml

        self.data_source_name: str | None = contract_yaml.data_source_file if contract_yaml else None
        data_source_location: dict[str, str] | None = (
            contract_yaml.dataset_locations.get(data_source.get_data_source_type_name())
            if contract_yaml.dataset_locations else None
        )
        self.dataset_prefix: list[str] | None = data_source.build_dataset_prefix(data_source_location)
        self.dataset_name: str | None = contract_yaml.dataset_name if contract_yaml else None
        metrics_resolver: MetricsResolver = MetricsResolver()
        self.checks: list[Check] = self._parse_checks(contract_yaml, metrics_resolver)
        self.metrics: list[Metric] = metrics_resolver.get_resolved_metrics()
        self.queries: list[Query] = self._build_queries()

    def _parse_checks(
        self,
        contract_yaml: ContractYaml,
        metrics_resolver: MetricsResolver,
    ) -> list[Check]:
        checks: list[Check] = []

        for check_yaml in contract_yaml.checks:
            checks.append(self._parse_check(
                contract_yaml=contract_yaml,
                check_yaml=check_yaml,
                column_yaml=None,
                metrics_resolver=metrics_resolver,
            ))
        for column_yaml in contract_yaml.columns:
            for check_yaml in column_yaml.checks:
                checks.append(self._parse_check(
                    contract_yaml=contract_yaml,
                    check_yaml=check_yaml,
                    column_yaml=column_yaml,
                    metrics_resolver=metrics_resolver,
                ))

        return checks

    def _parse_check(
        self,
        contract_yaml: ContractYaml,
        check_yaml: CheckYaml,
        column_yaml: ColumnYaml | None,
        metrics_resolver: MetricsResolver
    ) -> Check | None:
        return check_yaml.create_check(data_source=self.data_source, dataset_prefix=self.dataset_prefix,
                                       contract_yaml=contract_yaml, column_yaml=column_yaml, check_yaml=check_yaml,
                                       metrics_resolver=metrics_resolver)

    def _build_queries(self) -> list[Query]:
        queries: list[Query] = []
        aggregation_metrics: list[AggregationMetric] = []
        for check in self.checks:
            queries.extend(check.queries)
            aggregation_metrics.extend(check.aggregation_metrics)

        from soda_core.contracts.impl.check_types.schema_check_type import SchemaQuery
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
    def create(cls, check_yaml: CheckYaml, default_threshold: Threshold | None = None) -> Threshold | None:
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
        if self.type == ThresholdType.SINGLE_COMPARATOR:
            if isinstance(self.must_be_greater_than, Number):
                return f"{metric_name} > {self.must_be_greater_than}"
            if isinstance(self.must_be_greater_than_or_equal, Number):
                return f"{metric_name} >= {self.must_be_greater_than_or_equal}"
            if isinstance(self.must_be_less_than, Number):
                return f"{metric_name} < {self.must_be_less_than}"
            if isinstance(self.must_be_less_than_or_equal, Number):
                return f"{metric_name} <= {self.must_be_less_than_or_equal}"
            if isinstance(self.must_be, Number):
                return f"{metric_name} = {self.must_be}"
            if isinstance(self.must_not_be, Number):
                return f"{metric_name} != {self.must_not_be}"
        elif self.type == ThresholdType.INNER_RANGE:
            lower_bound = (str(self.must_be_greater_than) if isinstance(self.must_be_greater_than, Number)
                           else f"{self.must_be_greater_than_or_equal} (included)")
            upper_bound = (str(self.must_be_less_than) if isinstance(self.must_be_less_than, Number)
                           else f"{self.must_be_less_than_or_equal} (included)")
            return f"{metric_name} must be between {lower_bound} and {upper_bound}"
        elif self.type == ThresholdType.OUTER_RANGE:
            lower_bound = (str(self.must_be_less_than) if isinstance(self.must_be_less_than, Number)
                           else f"{self.must_be_less_than_or_equal} (included)")
            upper_bound = (str(self.must_be_greater_than) if isinstance(self.must_be_greater_than, Number)
                           else f"{self.must_be_greater_than_or_equal} (included)")
            return f"{metric_name} must be between {lower_bound} and {upper_bound}"

    def passes(self, value: Number) -> bool:
        return ((self.must_be_greater_than is None or value > self.must_be_greater_than)
           and (self.must_be_greater_than_or_equal is None or value >= self.must_be_greater_than_or_equal)
           and (self.must_be_less_than is None or value < self.must_be_less_than)
           and (self.must_be_less_than_or_equal is None or value <= self.must_be_less_than_or_equal)
           and (not (isinstance(self.must_be, Number) or isinstance(self.must_be, str)) or value == self.must_be)
           and (not (isinstance(self.must_not_be, Number) or isinstance(self.must_not_be, str)) or value != self.must_not_be)
        )


class Check:

    def __init__(
        self,
        contract_yaml: ContractYaml,
        column_yaml: ColumnYaml | None,
        check_yaml: CheckYaml,
        dataset_prefix: str | None,
        threshold: Threshold | None,
        summary: str
    ):
        self.logs: Logs = contract_yaml.logs

        self.data_source_name: str = contract_yaml.data_source_file
        self.dataset_prefix: str | None = dataset_prefix
        self.dataset_name: str = contract_yaml.dataset_name
        self.column_name: str | None = column_yaml.name if column_yaml else None
        self.type: str = check_yaml.type
        self.qualifier: str | None = check_yaml.qualifier
        self.check_yaml: CheckYaml = check_yaml
        self.threshold: Threshold | None = threshold
        self.summary: str | None = summary

        self.metrics: dict[str, Metric] = {}
        self.aggregation_metrics: list[AggregationMetric] = []
        self.queries: list[Query] = []

        self.skip: bool = False

    def skip_if_schema_is_not_matching(self, schema_check_result: 'SchemaCheckResult') -> None:
        if schema_check_result.dataset_does_not_exists():
            self.skip = True
        elif (self.column_name
              and schema_check_result.column_does_not_exist(self.column_name)
             ):
            self.skip = True

    @abstractmethod
    def evaluate(self) -> CheckResult:
        pass


class Metric:

    def __init__(
        self,
        data_source_name: str,
        dataset_prefix: list[str] | None,
        dataset_name: str | None,
        column_name: str | None,
        metric_type_name: str
    ):
        self.data_source_name: str = data_source_name
        self.dataset_prefix: list[str] | None = dataset_prefix
        self.dataset_name: str | None = dataset_name
        self.column_name: str | None = column_name
        self.type: str = metric_type_name
        # Initialized in the check.evaluate after queries are executed
        self.measured_value: any = None

    def _get_eq_properties(self, m: Metric) -> dict:
        return {
            k: v for k, v in m.__dict__.items() if k != "measured_value"
        }

    def __eq__(self, other):
        if type(other) != type(self):
            return False
        other_metric_properties: dict = self._get_eq_properties(other)
        self_metric_properties: dict = self._get_eq_properties(self)
        if other_metric_properties != self_metric_properties:
            return False
        return True


class AggregationMetric(Metric):

    def __init__(self, data_source_name: str, dataset_prefix: list[str] | None,
                 dataset_name: str | None, column_name: str | None, metric_type_name: str):
        super().__init__(data_source_name, dataset_prefix, dataset_name, column_name, metric_type_name)

    @abstractmethod
    def sql_expression(self) -> SqlExpression:
        pass

    def set_measured_value(self, param):
        pass


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
    def execute(self) -> None:
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

    def execute(self) -> None:
        sql = self.build_sql()
        query_result: QueryResult = self.data_source.execute_query(sql)
        row: tuple = query_result.rows[0]
        for i in range(0, len(self.aggregation_metrics)):
            aggregation_metric: AggregationMetric = self.aggregation_metrics[i]
            aggregation_metric.set_measured_value(row[i])

    def process_query_result(self, query_result: QueryResult) -> None:
        row: tuple = query_result.rows[0]
        for i in range(0, len(row)):
            self.aggregation_metrics[i].measured_value = row[i]


class Measurement:
    def __init__(self, metric: Metric, value: object):
        self.metric: Metric = metric
        self.value: object = value
