from __future__ import annotations

from soda.common.data_source import DataSource
from soda.common.data_source_parser import DataSourceParser
from soda.common.logs import Logs
from soda.common.yaml import YamlFile
from soda.contracts.contract_verification import ContractVerificationResult, ContractResult, \
    ContractVerificationBuilder, CheckResult, SodaException
from soda.contracts.impl.contract_parser import ContractParser
from soda.contracts.impl.contract_yaml import ContractYaml


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
            contract_parser: ContractParser = ContractParser(contract_yaml_file)
            contract_yaml: ContractYaml = contract_parser.parse(self.variables)
            contract: Contract = self.create_contract(contract_yaml)
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

    def create_contract(self, contract_yaml: ContractYaml) -> Contract:
        return Contract(contract_yaml=contract_yaml, logs=self.logs)

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
        check_dependency_resolver: CheckDependencyResolver = CheckDependencyResolver(contract.checks)
        while check_dependency_resolver.next_dependency_batch():
            checks_to_evaluate: list[Check] = check_dependency_resolver.get_next_dependency_batch()
            query_builder: QueryBuilder = QueryBuilder()
            for check_to_evaluate in checks_to_evaluate:
                resolved_query_metrics: list[QueryMetric] = []
                for check_query_metric in check_to_evaluate.query_metrics:
                    resolved_query_metric: QueryMetric = query_builder.resolve(check_query_metric)
                    resolved_query_metrics.append(resolved_query_metric)
                checks_to_evaluate.query_metrics = resolved_query_metrics
            self.execute_queries(query_builder.queries)

        check_results: list[CheckResult] = [
            check.create_check_result()
            for check in contract.checks
        ]

        return ContractResult(
            contract_yaml=contract.contract_yaml,
            check_results=check_results
        )

    def execute_queries(self, queries: list[Query]):
        for query in queries:
            query.execute(self)


class Contract:

    def __init__(self, contract_yaml: ContractYaml, logs: Logs):
        self.logs: Logs = logs
        self.contract_yaml: ContractYaml = contract_yaml
        self.data_source_name: str | None = contract_yaml.data_source_name if contract_yaml else None
        self.database_name: str | None = contract_yaml.database_name if contract_yaml else None
        self.schema_name: str | None = contract_yaml.schema_name if contract_yaml else None
        self.dataset_name: str | None = contract_yaml.dataset_name if contract_yaml else None
        self.checks: list[Check] = []


class QueryMetric:

    def __init__(self, type: str):
        self.type: str = type

    def __eq__(self, other):
        if type(other) != type(self):
            return False
        if other.__dict__ != self.__dict__:
            return False
        return True


class Query:

    def execute(self, data_source: DataSource) -> None:
        pass


class QueryBuilder:
    def __init__(self):
        self.query_metrics: list[QueryMetric] = []
        self.queries: list[Query] = []

    def resolve(self, check_query_metric: QueryMetric) -> QueryMetric:
        pass


class Check:

    def __init__(self):
        self.check_dependencies: list[Check] = []
        self.query_metrics: list[QueryMetric] = []

    def create_check_result(self) -> CheckResult:
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
