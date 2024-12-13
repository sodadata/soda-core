from __future__ import annotations

from abc import abstractmethod

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_connection import QueryResult
from soda_core.common.data_source_parser import DataSourceParser
from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlFile
from soda_core.contracts.contract_verification import ContractVerificationResult, ContractResult, \
    ContractVerificationBuilder, CheckResult
from soda_core.contracts.impl.check_types.schema_check_type import SchemaCheckResult, SchemaMetric, \
    SchemaQuery, SchemaCheck
from soda_core.contracts.impl.contract_yaml import ContractYaml, CheckYaml, ColumnYaml, CheckType, check_types


class ContractVerificationImpl:

    def __init__(
            self,
            data_source_yaml_file: YamlFile | None,
            data_source: object | None,
            spark_session: object | None,
            contract_files: list[YamlFile],
            soda_cloud_file: YamlFile | None,
            plugin_files: list[YamlFile],
            variables: dict[str, str],
            logs: Logs = Logs()
    ):
        self.data_source_yaml_file: YamlFile | None = data_source_yaml_file
        self.data_source: object | None = data_source
        self.spark_session: object | None = spark_session
        self.contract_files: list[YamlFile] = contract_files
        self.soda_cloud_file: YamlFile | None = soda_cloud_file
        self.plugin_files: list[YamlFile] = plugin_files
        self.variables: dict[str, str] = variables
        self.logs: Logs = logs
        self.data_source: DataSource = self._parse_data_source()
        self.contracts: list[Contract] = self._parse_contracts()

    def execute(self) -> ContractVerificationResult:
        contract_results: list[ContractResult] = []

        if isinstance(self.data_source, DataSource) and len(self.contracts) > 0:
            contract_results: list[ContractResult] = self.verify_contracts(self.contracts)
        else:
            self.logs.error("No data source configured")

        return ContractVerificationResult(
            logs=self.logs,
            variables=self.variables,
            contract_results=contract_results
        )

    def _parse_data_source(self) -> DataSource | None:
        if isinstance(self.data_source, DataSource):
            return self.data_source
        elif isinstance(self.data_source_yaml_file, YamlFile):
            data_source_yaml_file = self.data_source_yaml_file
            spark_session = self.spark_session
            data_source_parser = DataSourceParser(
                data_source_yaml_file=data_source_yaml_file,
                spark_session=spark_session
            )
            return data_source_parser.parse(self.variables)
        else:
            self.logs.error("No data source provided")

    def _parse_contracts(self) -> list[Contract]:
        contracts: list[Contract] = []
        for contract_yaml_file in self.contract_files:
            contract_yaml_file.parse(variables=self.variables)
            contract_yaml: ContractYaml = ContractYaml(contract_yaml_file=contract_yaml_file)
            contract: Contract = Contract(contract_yaml=contract_yaml, data_source=self.data_source, logs=self.logs)
            contracts.append(contract)
        return contracts

    def _parse_soda_cloud(self, contract_verification_builder: ContractVerificationBuilder) -> None:
        soda_cloud_file: YamlFile | None = contract_verification_builder.soda_cloud_file
        if isinstance(soda_cloud_file, YamlFile) and soda_cloud_file.exists():
            soda_cloud_file.parse(contract_verification_builder.variables)
            self.soda_cloud = SodaCloud(soda_cloud_file)

    def _parse_plugins(self, contract_verification_builder) -> None:
        plugin_files_by_type: dict[str, list[YamlFile]] = {}
        for plugin_file in contract_verification_builder.plugin_files:
            plugin_file.parse(self.variables)
            if isinstance(plugin_file.dict, dict):
                plugin: str | None = plugin_file.dict.get("plugin")
                if isinstance(plugin, str):
                    plugin_files_by_type.setdefault(plugin, []).append(plugin_file)
                else:
                    self.logs.error(f"Key plugin is required in plugin YAML files")
            else:
                self.logs.error(f"Could not read plugin YAML file {plugin_file.file_path}")
        for plugin_name, plugin_yaml_files in plugin_files_by_type.items():
            for plugin_yaml_file in plugin_yaml_files:
                plugin_yaml_file.parse(self.variables)
            plugin: Plugin = Plugin.create(plugin_name, plugin_yaml_files, self.logs)
            if isinstance(plugin, Plugin):
                self.plugins.append(plugin)
            else:
                self.logs.error(f"Could not load plugin {plugin_name}")

    def verify_contracts(self, contracts: list[Contract]) -> list[ContractResult]:
        contract_results: list[ContractResult] = []
        self.data_source.data_source_connection.open_connection()
        try:
            if len(contracts) > 0:
                for contract in contracts:
                    contract_result: ContractResult = self.verify_contract(contract)
                    contract_results.append(contract_result)
            else:
                self.logs.error("No contracts specified")
        finally:
            self.data_source.data_source_connection.close_connection()
        return contract_results

    def verify_contract(self, contract: Contract) -> ContractResult:
        # Ensures that all contract->check->metrics that are equal are merged into the same Python object
        self._resolve_metrics(contract)

        self.checks_not_evaluated: list[Check] = checks
        self.checks_evaluating_batch: list[Check] = []
        self.checks_evaluated: list[Check] = []

        schema_check_results: list[CheckResult] = self._execute_schema_checks(contract)
        schema_check_result: SchemaCheckResult | None = next(
            (check_result for check_result in schema_check_results if isinstance(check_result, SchemaCheckResult)),
            None
        )
        schema_check_results: list[CheckResult] = self._execute_other_checks(contract, schema_check_result)
        all_check_results: list[CheckResult] = schema_check_results + schema_check_results
        return ContractResult(
            contract_yaml=contract.contract_yaml,
            check_results=all_check_results
        )

    def _execute_schema_checks(self,  contract: Contract) -> list[CheckResult]:
        """
        Executes all the schema checks (typically 1) in the contract, appends the schema
        check results to the check_results parameter and returns the first schema measurement,
        if there is one so that the other, subsequent check evaluations can use the schema measurement.
        """
        schema_checks: list[Check] = [
            check for check in contract.checks if check.check_type.get_check_type_name() == "schema"
        ]
        return self.execute_checks(checks=schema_checks)

    def _execute_other_checks(self, contract: Contract, schema_check_result: SchemaCheckResult | None) -> list[CheckResult]:
        other_checks: list[Check] = [
            check for check in contract.checks if not isinstance(check, SchemaCheck)
        ]
        for other_check in other_checks:
            other_check.skip_if_schema_is_not_matching(schema_check_result)
        other_checks_not_skipped: list[Check] = [
            check for check in contract.checks if not check.skip
        ]
        return self.execute_checks(checks=other_checks_not_skipped)

    def execute_checks(self, checks: list[Check]) -> list[CheckResult]:
        query_builder: QueryBuilder = QueryBuilder()
        query_builder.build_queries(checks)
        query_builder.execute_queries(self.data_source)
        return [
            check.evaluate()
            for check in checks
        ]

    def build_queries(self, checks: list[Check]) -> None:
        metrics: list[Metric] = self._resolve_metrics(checks)
        for metric in metrics:
            self._ensure_query_for_metric(metric)

    def execute(self, data_source: DataSource) -> QueryResult:
        query_sql: str = self.build_query_sql()
        return data_source.data_source_connection.execute_query(query_sql)

    @abstractmethod
    def build_query_sql(self) -> str:
        pass

    @abstractmethod
    def process_query_result(self, query_result: QueryResult) -> None:
        """
        Push the query result into the metrics
        """
        pass


class CheckDependencyResolver:
    def __init__(self, checks: list[Check]):
        self.checks_not_evaluated: list[Check] = checks
        self.checks_evaluating_batch: list[Check] = []
        self.checks_evaluated: list[Check] = []

    def next_dependency_batch(self) -> bool:
        self.checks_evaluated.extend(self.checks_evaluating_batch)
        self.checks_evaluating_batch = []
        for check_not_evaluated in self.checks_not_evaluated:
            if self.has_all_dependencies_available(check_not_evaluated):
                self.checks_not_evaluated.remove(check_not_evaluated)
                self.checks_evaluating_batch.append(check_not_evaluated)
        return len(self.checks_evaluating_batch) > 0

    def has_all_dependencies_available(self, check_not_evaluated: Check) -> bool:
        return all(
            check_dependency in self.checks_evaluated
            for check_dependency in check_not_evaluated.check_dependencies
        )

    def get_next_dependency_batch(self) -> list[Check]:
        return self.checks_evaluating_batch


class Contract:

    def __init__(self, contract_yaml: ContractYaml, data_source: DataSource, logs: Logs):
        self.logs: Logs = logs
        self.data_source: DataSource = data_source
        self.contract_yaml: ContractYaml = contract_yaml
        self.data_source_name: str | None = contract_yaml.data_source_name if contract_yaml else None
        self.database_name: str | None = contract_yaml.database_name if contract_yaml else None
        self.schema_name: str | None = contract_yaml.schema_name if contract_yaml else None
        self.dataset_name: str | None = contract_yaml.dataset_name if contract_yaml else None
        metrics_resolver: MetricsResolver = MetricsResolver()
        queries_factory: QueriesFactory = QueriesFactory()
        self.checks: list[Check] = self._parse_checks(contract_yaml, metrics_resolver, queries_factory)
        self.metrics: list[Metric] =
        self.queries: list[Query] = self._build_queries(self.metrics)

    def _parse_checks(
        self,
        contract_yaml: ContractYaml,
        metrics_resolver: MetricsResolver,
        queries_factory: QueriesFactory
    ) -> list[Check]:
        checks: list[Check] = []

        for check_yaml in contract_yaml.checks:
            checks.append(self._parse_check(
                contract_yaml=contract_yaml,
                check_yaml=check_yaml,
                column_yaml=None,
                metrics_resolver=metrics_resolver,
                query_factory=query_factory
            ))
        for column_yaml in contract_yaml.columns:
            for check_yaml in column_yaml.checks:
                checks.append(self._parse_check(
                    contract_yaml=contract_yaml,
                    check_yaml=check_yaml,
                    column_yaml=column_yaml,
                    metrics_resolver=metrics_resolver,
                    query_factory=query_factory
                ))

        return checks

    def _parse_check(
        self,
        contract_yaml: ContractYaml,
        check_yaml: CheckYaml,
        column_yaml: ColumnYaml | None,
    ) -> Check | None:

        self.check_type: CheckType | None = check_types.get(check_yaml.type)
        if self.check_type is None:
            self.logs.error(f"Check type '{check_yaml.type}' is not supported")
        else:
            return self.check_type.create_check(
                contract_yaml=contract_yaml,
                check_yaml=check_yaml,
                column_yaml=column_yaml,
                metrics_resolver=metrics_resolver
            )

    def _resolve_metrics(self, checks: list[Check]) -> list[Metric]:
        metrics: list[Metric] = []

        for check in checks:
            check_metrics: list[Metric] = []
            for metric in check.metrics:
                existing_metric: Metric | None = next((m for m in metrics if m == metric), None)
                if existing_metric:
                    check_metrics.append(existing_metric)
                else:
                    metrics.append(metric)
                    check_metrics.append(metric)
            check.metrics = check_metrics

        return metrics

    def _build_queries(self, metrics: list[Metric]) -> list[Query]:
        query_builder: QueryBuilder = QueryBuilder()
        for metric in metrics:
            metric.ensure_queries(query_builder)
        return query_builder.build()


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


class QueryFactory:

    def __init__(self):
        self.schema_queries: list[SchemaQuery] = []
        self.aggregations: list[Aggregation] = []
        self.other_query_builders: list[QueryBuilder] = []

    def add_query_builder(self, query_builder: QueryBuilder):
        if isinstance(query_builder, SchemaQueryBuilder):
            self.schema_query_builders.append(query_builder)
        else:
            self.other_query_builders.append(query_builder)

    def add_aggregation(self, aggregation: Aggregation):
        self.aggregations.append(aggregation)

    def build_queries(self) -> list[Query]:
        return (self._build_queries(self.schema_query_builders)
                + self._build_aggregation_queries(self.aggregations)
                + self._build_queries(self.other_query_builders))

    def _build_queries(self, query_builders: list[QueryBuilder]) -> list[Query]:
        return [query_builder.build_query() for query_builder in query_builders]

    def _build_aggregation_queries(self, aggregations: list[Aggregation]) -> list[Query]:
        pass


class Check:

    def __init__(
        self,
        contract_yaml: ContractYaml,
        check_yaml: CheckYaml,
        column_yaml: ColumnYaml | None,
    ):
        self.logs: Logs = contract_yaml.logs
        self.contract_yaml: ContractYaml = contract_yaml
        self.check_yaml: CheckYaml = check_yaml
        self.column_yaml: ColumnYaml = column_yaml

        self.data_source_name: str = contract_yaml.data_source_name
        self.database_name: str | None = contract_yaml.database_name
        self.schema_name: str | None = contract_yaml.schema_name
        self.dataset_name: str = contract_yaml.dataset_name
        self.column_name: str | None = column_yaml.name if column_yaml else None

        self.skip: bool = False
        self.metrics: list[Metric] = self._create_metrics()

    @abstractmethod
    def _create_metrics(self) -> list[Metric]:
        pass

    def skip_if_schema_is_not_matching(self, schema_check_result: SchemaCheckResult) -> None:
        if schema_check_result.dataset_does_not_exists():
            self.skip = True
        elif (self.column_yaml
              and self.column_yaml.name
              and schema_check_result.column_does_not_exist(self.column_yaml.name)
             ):
            self.skip = True

    @abstractmethod
    def evaluate(self) -> CheckResult:
        pass


class Metric:

    def __init__(
        self,
        data_source_name: str,
        database_name: str | None,
        schema_name: str | None,
        dataset_name: str | None,
        column_name: str | None,
        metric_type_name: str
    ):
        self.data_source_name: str = data_source_name
        self.database_name: str | None = database_name
        self.schema_name: str | None = schema_name
        self.dataset_name: str | None = dataset_name
        self.column_name: str | None = column_name
        self.type: str = metric_type_name

    def __eq__(self, other):
        if type(other) != type(self):
            return False
        if other.__dict__ != self.__dict__:
            return False
        return True

    def ensure_queries(self, queries_factory: QueriesFactory) -> None:
        pass


class Aggregation:
    pass


class Query:

    def __init__(self):
        self.sql: str | None = None
        self.metrics: list[Metric] = []


class AggregationQuery(Query):

    def __init__(self):
        super().__init__()

    def is_full(self) -> bool:
        return len(self.metrics) > 50

    def build_query_sql(self) -> str:
        pass

    def process_query_result(self, query_result: QueryResult) -> None:
        pass


class Measurement:
    def __init__(self, metric: Metric, value: object):
        self.metric: Metric = metric
        self.value: object = value
