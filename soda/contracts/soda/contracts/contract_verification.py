from __future__ import annotations

import importlib
from typing import Iterator

from soda.contracts.contract import Contract, ContractResult
from soda.contracts.impl.contract_verification_impl import (
    FileVerificationDataSource,
    SparkVerificationDataSource,
    VerificationDataSource, SparkConfiguration,
)
from soda.contracts.impl.logs import Logs
from soda.contracts.impl.plugin import Plugin
from soda.contracts.impl.soda_cloud import SodaCloud
from soda.contracts.impl.yaml_helper import YamlFile


class ContractVerificationBuilder:

    def __init__(self):
        self.logs: Logs = Logs()
        self.data_source_yaml_files: list[YamlFile] = []
        self.spark_configurations: list[SparkConfiguration] = []
        self.contract_files: list[YamlFile] = []
        self.soda_cloud_files: list[YamlFile] = []
        self.plugin_files: list[YamlFile] = []
        self.variables: dict[str, str] = {}

    def with_contract_yaml_file(self, contract_yaml_file_path: str) -> ContractVerificationBuilder:
        if not isinstance(contract_yaml_file_path, str):
            self.logs.error(
                message=f"In ContractVerificationBuilder, parameter contract_yaml_file_path must be a string, but was {contract_yaml_file_path} ({type(contract_yaml_file_path)})"
            )
        self.contract_files.append(YamlFile(yaml_file_path=contract_yaml_file_path, logs=self.logs))
        return self

    def with_contract_yaml_str(self, contract_yaml_str: str) -> ContractVerificationBuilder:
        assert isinstance(contract_yaml_str, str)
        self.contract_files.append(YamlFile(yaml_str=contract_yaml_str, logs=self.logs))
        return self

    def with_contract_yaml_dict(self, contract_yaml_dict: dict) -> ContractVerificationBuilder:
        assert isinstance(contract_yaml_dict, dict)
        self.contract_files.append(YamlFile(yaml_dict=contract_yaml_dict, logs=self.logs))
        return self

    def with_data_source_yaml_file(self, data_source_yaml_file_path: str) -> ContractVerificationBuilder:
        assert isinstance(data_source_yaml_file_path, str)
        data_source_yaml_file = YamlFile(yaml_file_path=data_source_yaml_file_path, logs=self.logs)
        self.data_source_yaml_files.append(data_source_yaml_file)
        return self

    def with_data_source_yaml_str(self, data_source_yaml_str: str) -> ContractVerificationBuilder:
        assert isinstance(data_source_yaml_str, str)
        data_source_yaml_file = YamlFile(logs=self.logs, yaml_str=data_source_yaml_str)
        self.data_source_yaml_files.append(data_source_yaml_file)
        return self

    def with_data_source_yaml_dict(self, data_source_yaml_dict: dict) -> ContractVerificationBuilder:
        assert isinstance(data_source_yaml_dict, dict)
        data_source_yaml_file = YamlFile(logs=self.logs, yaml_dict=data_source_yaml_dict)
        self.data_source_yaml_files.append(data_source_yaml_file)
        return self

    def with_data_source_spark_session(
        self, spark_session: object, data_source_yaml_dict: dict | None = None
    ) -> ContractVerificationBuilder:
        assert isinstance(spark_session, object)
        assert isinstance(data_source_yaml_dict, dict) or data_source_yaml_dict is None
        self.spark_configurations.append(SparkConfiguration(
            spark_session=spark_session,
            data_source_yaml_dict=data_source_yaml_dict if isinstance(data_source_yaml_dict, dict) else {},
            logs=self.logs
        ))
        return self

    def with_variable(self, key: str, value: str) -> ContractVerificationBuilder:
        self.variables[key] = value
        return self

    def with_variables(self, variables: dict[str, str]) -> ContractVerificationBuilder:
        if isinstance(variables, dict):
            self.variables.update(variables)
        return self

    def with_soda_cloud_yaml_file(self, soda_cloud_yaml_file_path: str) -> ContractVerificationBuilder:
        assert isinstance(soda_cloud_yaml_file_path, str)
        self.soda_cloud_files.append(YamlFile(yaml_file_path=soda_cloud_yaml_file_path, logs=self.logs))
        return self

    def with_soda_cloud_yaml_str(self, soda_cloud_yaml_str: str) -> ContractVerificationBuilder:
        assert isinstance(soda_cloud_yaml_str, str)
        self.soda_cloud_files.append(YamlFile(yaml_str=soda_cloud_yaml_str, logs=self.logs))
        return self

    def with_soda_cloud_yaml_dict(self, soda_cloud_yaml_dict: dict) -> ContractVerificationBuilder:
        assert isinstance(soda_cloud_yaml_dict, dict)
        self.soda_cloud_files.append(YamlFile(yaml_dict=soda_cloud_yaml_dict, logs=self.logs))
        return self

    def with_plugin_yaml_file(self, plugin_yaml_file_path: str) -> ContractVerificationBuilder:
        assert isinstance(plugin_yaml_file_path, str)
        self.plugin_files.append(YamlFile(yaml_file_path=plugin_yaml_file_path, logs=self.logs))
        return self

    def with_plugin_yaml_str(self, plugin_yaml_str: str) -> ContractVerificationBuilder:
        assert isinstance(plugin_yaml_str, str)
        self.plugin_files.append(YamlFile(yaml_str=plugin_yaml_str, logs=self.logs))
        return self

    def with_plugin_yaml_dict(self, plugin_yaml_dict: dict) -> ContractVerificationBuilder:
        assert isinstance(plugin_yaml_dict, dict)
        self.plugin_files.append(YamlFile(yaml_dict=plugin_yaml_dict, logs=self.logs))
        return self

    def build(self) -> ContractVerification:
        return ContractVerification(contract_verification_builder=self)

    def execute(self) -> ContractVerificationResult:
        contract_verification: ContractVerification = self.build()
        return contract_verification.execute()


class ContractVerification:

    @classmethod
    def builder(cls) -> ContractVerificationBuilder:
        return ContractVerificationBuilder()

    def __init__(self, contract_verification_builder: ContractVerificationBuilder):
        self.logs: Logs = contract_verification_builder.logs
        self.variables: dict[str, str] = contract_verification_builder.variables
        self.contracts: list[Contract] = []
        self.contract_results: list[ContractResult] = []
        self.soda_cloud: SodaCloud | None = None
        self.verification_data_sources = self._parse_verification_data_sources(contract_verification_builder)
        self.plugins: list[Plugin] = []

        # parse the contract files and add them to the matching verification data source
        for contract_file in contract_verification_builder.contract_files:
            contract_file.parse(self.variables)
            if contract_file.is_ok():
                contract_data_source_name: str | None = contract_file.dict.get("data_source")

                verification_data_source = self.verification_data_sources.get(contract_data_source_name)

                contract: Contract = Contract.create(
                    data_source=verification_data_source.data_source,
                    contract_file=contract_file,
                    variables=self.variables,
                    soda_cloud=self.soda_cloud,
                    logs=contract_file.logs,
                )
                verification_data_source.add_contract(contract)
                self.contracts.append(contract)

        for soda_cloud_file in contract_verification_builder.soda_cloud_files:
            if soda_cloud_file.exists():
                soda_cloud_file.parse(self.variables)
                self.soda_cloud = SodaCloud(soda_cloud_file)
                break

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

    def _parse_verification_data_sources(self, contract_verification_builder) -> VerificationDataSources:
        return VerificationDataSources(contract_verification_builder)

    def __str__(self) -> str:
        return str(self.logs)

    def execute(self) -> ContractVerificationResult:
        all_contract_results: list[ContractResult] = []
        for verification_data_source in self.verification_data_sources:
            data_source_contract_results: list[ContractResult] = self.ensure_open_and_verify_contracts(verification_data_source)
            all_contract_results.extend(data_source_contract_results)
        return ContractVerificationResult(
            logs=self.logs, variables=self.variables, contract_results=all_contract_results
        )

    def ensure_open_and_verify_contracts(self, verification_data_source: VerificationDataSource) -> list[ContractResult]:
        """
        Ensures that the data source has an open connection and then invokes self.__verify_contracts()
        """
        if verification_data_source.requires_with_block():
            with verification_data_source.data_source as d:
                return self.verify_contracts()
        else:
            return self.verify_contracts()

    def verify_contracts(self):
        """
        Assumes the data source has an open connection
        """
        contract_results: list[ContractResult] = []
        for contract in self.contracts:
            # TODO ensure proper initialization of contract.soda_cloud
            contract.soda_cloud = self.soda_cloud
            contract_result: ContractResult = contract.verify()
            contract_results.append(contract_result)
            for plugin in self.plugins:
                plugin.process_contract_results(contract_result)
        return contract_results


class VerificationDataSources:
    def __init__(self, contract_verification_builder: ContractVerificationBuilder):
        self.verification_data_sources_by_name: dict[str, VerificationDataSource] = {}
        # The purpose of the undefined verification data_source is to ensure that we still capture
        # all the parsing errors in the logs, even if there is no data_source associated
        self.undefined_verification_data_source = VerificationDataSource()
        self.single_verification_data_source: VerificationDataSource | None = None
        self.logs: Logs = contract_verification_builder.logs

        # Parse data sources
        variables: dict[str, str] = contract_verification_builder.variables
        for data_source_yaml_file in contract_verification_builder.data_source_yaml_files:
            data_source_yaml_file.parse(variables)
            verification_data_source: VerificationDataSource = FileVerificationDataSource(
                data_source_yaml_file=data_source_yaml_file
            )
            self.add(verification_data_source)
        for spark_configuration in contract_verification_builder.spark_configurations:
            verification_data_source: VerificationDataSource = SparkVerificationDataSource(
                spark_configuration=spark_configuration
            )
            self.add(verification_data_source)

    def get(self, contract_data_source_name: str | None) -> VerificationDataSource:
        if isinstance(contract_data_source_name, str):
            verification_data_source = self.verification_data_sources_by_name.get(contract_data_source_name)
            if verification_data_source:
                return verification_data_source
            else:
                self.logs.error(f"Data source '{contract_data_source_name}' not configured")
                return self.undefined_verification_data_source

        if self.single_verification_data_source:
            # no data source specified in the contract
            # and a single data source was specified in the verification
            return self.single_verification_data_source

        return self.undefined_verification_data_source

    def add(self, verification_data_source: VerificationDataSource):
        self.verification_data_sources_by_name[verification_data_source.data_source.data_source_name] = verification_data_source
        # update self.single_verification_data_source because the number of verification data_sources changed
        data_sources_count = len(self.verification_data_sources_by_name)
        if data_sources_count == 1:
            self.single_verification_data_source = next(iter(self.verification_data_sources_by_name.values()))
        elif data_sources_count == 0:
            self.single_verification_data_source = self.undefined_verification_data_source

    def __iter__(self) -> Iterator[VerificationDataSource]:
        return iter(self.verification_data_sources_by_name.values())


class ContractVerificationResult:
    def __init__(self, logs: Logs, variables: dict[str, str], contract_results: list[ContractResult]):
        self.logs: Logs = logs
        self.variables: dict[str, str] = variables
        self.contract_results: list[ContractResult] = contract_results

    def failed(self) -> bool:
        """
        Returns True if there are execution errors or if there are check failures.
        """
        return not self.passed()

    def passed(self) -> bool:
        """
        Returns True if there are no execution errors and no check failures.
        """
        return not self.logs.has_errors() and all(contract_result.passed() for contract_result in self.contract_results)

    def has_errors(self) -> bool:
        return self.logs.has_errors()

    def has_failures(self) -> bool:
        return any(contract_result.failed() for contract_result in self.contract_results)

    def is_ok(self) -> bool:
        return not self.has_errors() and not self.has_failures()

    def assert_ok(self) -> ContractVerificationResult:
        errors_str: str | None = self.logs.get_errors_str() if self.logs.get_errors() else None
        if errors_str or any(contract_result.failed() for contract_result in self.contract_results):
            raise SodaException(message=errors_str, contract_verification_result=self)
        return self

    def __str__(self) -> str:
        blocks: list[str] = [str(self.logs)]
        for contract_result in self.contract_results:
            blocks.extend(self.__format_contract_results_with_heading(contract_result))
        return "\n".join(blocks)

    @classmethod
    def __format_contract_results_with_heading(cls, contract_result: ContractResult) -> list[str]:
        return [f"# Contract results for {contract_result.contract.dataset}", str(contract_result)]


class SodaException(Exception):
    """
    See also adr/03_exceptions_vs_error_logs.md
    """

    def __init__(
        self, message: str | None = None, contract_verification_result: ContractVerificationResult | None = None
    ):
        self.contract_verification_result: ContractVerificationResult | None = contract_verification_result
        message_parts: list[str] = []
        if message:
            message_parts.append(message)
        if self.contract_verification_result:
            message_parts.append(str(self.contract_verification_result))
        exception_message: str = "\n".join(message_parts)
        super().__init__(exception_message)
