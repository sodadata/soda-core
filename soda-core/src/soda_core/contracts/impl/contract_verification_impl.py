from __future__ import annotations

from abc import abstractmethod, ABC

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_connection import QueryResult
from soda_core.common.data_source_parser import DataSourceParser
from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlFile
from soda_core.contracts.contract_verification import ContractVerificationResult, ContractResult, \
    ContractVerificationBuilder, CheckResult
from soda_core.contracts.impl.contract_yaml import ContractYaml, CheckYaml, ColumnYaml, CheckType


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
        for query in contract.queries:
            query.execute(self.data_source)

        check_results: list[CheckResult] = []
        for check in contract.checks:
            check_result: CheckResult = check.evaluate()
            check_results.append(check_result)

        return ContractResult(
            contract_yaml=contract.contract_yaml,
            check_results=check_results
        )


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
        return check_yaml.create_check(
            check_yaml=check_yaml,
            column_yaml=column_yaml,
            contract_yaml=contract_yaml,
            metrics_resolver=metrics_resolver,
            data_source=self.data_source
        )

    def _build_queries(self) -> list[Query]:
        queries: list[Query] = []
        aggregations: list[Aggregation] = []
        for check in self.checks:
            queries.extend(check.queries)
            aggregations.extend(check.aggregations)

        from soda_core.contracts.impl.check_types.schema_check_type import SchemaQuery
        schema_queries: list[SchemaQuery] = []
        other_queries: list[SchemaQuery] = []
        for query in queries:
            if isinstance(query, SchemaQuery):
                schema_queries.append(query)
            else:
                other_queries.append(query)

        aggregation_queries: list[AggregationQuery] = []
        for aggregation in aggregations:
            if len(aggregation_queries) == 0 or not aggregation_queries[-1].can_add(aggregation):
                aggregations.append(AggregationQuery())
            aggregation_queries[-1].append_aggregation(aggregation)

        for aggregation_query in aggregation_queries:
            aggregation_query.build()

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


class AggregationQueries:

    def __init__(self):
        self.aggregations: list[Aggregation] = []

    def add_aggregation(self, aggregation: Aggregation) -> None:
        self.aggregations.append(aggregation)

    def build(self) -> list[Query]:
        pass


class Check:

    def __init__(
        self,
        contract_yaml: ContractYaml,
        column_yaml: ColumnYaml | None,
        check_yaml: CheckYaml,
    ):
        self.logs: Logs = contract_yaml.logs

        self.data_source_name: str = contract_yaml.data_source_name
        self.database_name: str | None = contract_yaml.database_name
        self.schema_name: str | None = contract_yaml.schema_name
        self.dataset_name: str = contract_yaml.dataset_name
        self.column_name: str | None = column_yaml.name if column_yaml else None
        self.type: str = check_yaml.type
        self.qualifier: str | None = check_yaml.qualifier

        self.metrics: list[Metric] = []
        self.queries: list[Query] = []
        self.aggregations: list[Aggregation] = []

        self.skip: bool = False

    def skip_if_schema_is_not_matching(self, schema_check_result: SchemaCheckResult) -> None:
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


class Query(ABC):

    def __init__(
        self,
        sql: str,
        metrics: list[Metric]
    ):
        self.sql: str = sql
        self.metrics: list[Metric] = metrics

    @abstractmethod
    def execute(self, data_source: DataSource) -> None:
        pass


class Aggregation:
    pass


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