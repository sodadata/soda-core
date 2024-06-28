from __future__ import annotations

import logging

from soda.common import logs as soda_core_logs
from soda.scan import Scan
from soda.scan import logger as scan_logger

from soda.contracts.contract import Contract, ContractResult
from soda.contracts.impl.customized_sodacl_soda_cloud import CustomizedSodaClCloud
from soda.contracts.impl.data_source import DataSource, SparkConfiguration
from soda.contracts.impl.logs import Location, Log, LogLevel, Logs
from soda.contracts.impl.plugin import Plugin
from soda.contracts.impl.soda_cloud import SodaCloud
from soda.contracts.impl.yaml_helper import QuotingSerializer, YamlFile, YamlHelper

logger = logging.getLogger(__name__)


class ContractVerificationBuilder:

    def __init__(self):
        self.logs: Logs = Logs()
        self.data_source_yaml_file: YamlFile | None = None
        self.spark_configuration: SparkConfiguration | None = None
        self.contract_files: list[YamlFile] = []
        self.soda_cloud_file: YamlFile | None = None
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
        if self.data_source_yaml_file is not None:
            self.logs.error("Duplicate data source definition. Ignoring previous data sources.")
        self.data_source_yaml_file = YamlFile(yaml_file_path=data_source_yaml_file_path, logs=self.logs)
        return self

    def with_data_source_yaml_str(self, data_source_yaml_str: str) -> ContractVerificationBuilder:
        assert isinstance(data_source_yaml_str, str)
        if self.data_source_yaml_file is not None:
            self.logs.error("Duplicate data source definition. Ignoring previous data sources.")
        self.data_source_yaml_file = YamlFile(logs=self.logs, yaml_str=data_source_yaml_str)
        return self

    def with_data_source_yaml_dict(self, data_source_yaml_dict: dict) -> ContractVerificationBuilder:
        assert isinstance(data_source_yaml_dict, dict)
        if self.data_source_yaml_file is not None:
            self.logs.error("Duplicate data source definition. Ignoring previous data sources.")
        self.data_source_yaml_file = YamlFile(logs=self.logs, yaml_dict=data_source_yaml_dict)
        return self

    def with_data_source_spark_session(
        self, spark_session: object, data_source_yaml_dict: dict | None = None
    ) -> ContractVerificationBuilder:
        assert isinstance(spark_session, object)
        assert isinstance(data_source_yaml_dict, dict) or data_source_yaml_dict is None
        self.spark_configuration = SparkConfiguration(
            spark_session=spark_session,
            data_source_yaml_dict=data_source_yaml_dict if isinstance(data_source_yaml_dict, dict) else {},
            logs=self.logs,
        )
        return self

    def with_soda_cloud_yaml_file(self, soda_cloud_yaml_file_path: str) -> ContractVerificationBuilder:
        assert isinstance(soda_cloud_yaml_file_path, str)
        if self.soda_cloud_file is not None:
            self.logs.error("Duplicate Soda Cloud definition. Ignoring previous data sources.")
        self.soda_cloud_file = YamlFile(yaml_file_path=soda_cloud_yaml_file_path, logs=self.logs)
        return self

    def with_soda_cloud_yaml_str(self, soda_cloud_yaml_str: str) -> ContractVerificationBuilder:
        assert isinstance(soda_cloud_yaml_str, str)
        if self.soda_cloud_file is not None:
            self.logs.error("Duplicate Soda Cloud definition. Ignoring previous data sources.")
        self.soda_cloud_file = YamlFile(yaml_str=soda_cloud_yaml_str, logs=self.logs)
        return self

    def with_soda_cloud_yaml_dict(self, soda_cloud_yaml_dict: dict) -> ContractVerificationBuilder:
        assert isinstance(soda_cloud_yaml_dict, dict)
        if self.soda_cloud_file is not None:
            self.logs.error("Duplicate Soda Cloud definition. Ignoring previous data sources.")
        self.soda_cloud_file = YamlFile(yaml_dict=soda_cloud_yaml_dict, logs=self.logs)
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

    def with_variable(self, key: str, value: str) -> ContractVerificationBuilder:
        self.variables[key] = value
        return self

    def with_variables(self, variables: dict[str, str]) -> ContractVerificationBuilder:
        if isinstance(variables, dict):
            self.variables.update(variables)
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
        self.data_source: DataSource | None = None
        self.contracts: list[Contract] = []
        self.soda_cloud: SodaCloud | None = None
        self.plugins: list[Plugin] = []
        self.contract_results: list[ContractResult] = []

        self._initialize_data_source(contract_verification_builder)
        self._initialize_soda_cloud(contract_verification_builder)
        self._initialize_contracts(contract_verification_builder)
        self._initialize_plugins(contract_verification_builder)

    def _initialize_data_source(self, contract_verification_builder: ContractVerificationBuilder) -> None:
        data_source_yaml_file: YamlFile | None = contract_verification_builder.data_source_yaml_file
        spark_configuration: SparkConfiguration | None = contract_verification_builder.spark_configuration
        if isinstance(data_source_yaml_file, YamlFile):
            data_source_yaml_file.parse(contract_verification_builder.variables)
            self.data_source = DataSource.from_yaml_file(data_source_yaml_file)
        elif isinstance(spark_configuration, SparkConfiguration):
            self.data_source = DataSource.from_spark_session(spark_configuration)

    def _initialize_contracts(self, contract_verification_builder: ContractVerificationBuilder) -> None:
        for contract_file in contract_verification_builder.contract_files:
            contract_file.parse(self.variables)
            if contract_file.is_ok():
                contract: Contract = Contract(
                    contract_file=contract_file,
                    logs=contract_file.logs,
                )
                self.contracts.append(contract)

    def _initialize_soda_cloud(self, contract_verification_builder: ContractVerificationBuilder) -> None:
        soda_cloud_file: YamlFile | None = contract_verification_builder.soda_cloud_file
        if isinstance(soda_cloud_file, YamlFile) and soda_cloud_file.exists():
            soda_cloud_file.parse(contract_verification_builder.variables)
            self.soda_cloud = SodaCloud(soda_cloud_file)

    def _initialize_plugins(self, contract_verification_builder) -> None:
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

    def __str__(self) -> str:
        return str(self.logs)

    def execute(self) -> ContractVerificationResult:
        contract_results: list[ContractResult] = []

        if self.data_source is not None:
            with self.data_source:
                if len(self.contracts) > 0:
                    for contract in self.contracts:
                        contract_result: ContractResult = self._verify(contract)
                        contract_results.append(contract_result)
                        for plugin in self.plugins:
                            plugin.process_contract_results(contract_result)
                else:
                    self.logs.error("No contracts configured")
        else:
            self.logs.error("No data source configured")

        return ContractVerificationResult(logs=self.logs, variables=self.variables, contract_results=contract_results)

    def _verify(self, contract: Contract) -> ContractResult:
        scan = Scan()

        scan_logs = soda_core_logs.Logs(logger=scan_logger)
        scan.set_verbose(True)

        sodacl_yaml_str: str | None = None
        try:
            sodacl_yaml_str = self._generate_sodacl_yaml_str(contract)
            logger.debug("Generated SodaCL:")
            logger.debug(sodacl_yaml_str)

            if sodacl_yaml_str and hasattr(self.data_source, "sodacl_data_source"):
                scan._logs = scan_logs

                # This assumes the connection is a DataSourceConnection
                sodacl_data_source = self.data_source.sodacl_data_source
                # Execute the contract SodaCL in a scan
                scan.set_data_source_name(sodacl_data_source.data_source_name)
                # noinspection PyProtectedMember
                scan._data_source_manager.data_sources[self.data_source.data_source_name] = sodacl_data_source

                if self.soda_cloud:
                    scan_definition_name_parts: list[str] = [
                        self.data_source.data_source_name,
                        contract.database_name,
                        contract.schema_name,
                        contract.dataset_name,
                    ]
                    parts_str: str = "/".join([part for part in scan_definition_name_parts if part is not None])
                    scan_definition_name = f"dataset://{parts_str}"
                    scan.set_scan_definition_name(scan_definition_name)

                    prefix_parts: list[str] = [contract.database_name, contract.schema_name]
                    prefix = ".".join([prefix_part for prefix_part in prefix_parts if isinstance(prefix_part, str)])

                    default_data_source_properties = {"type": "spark_df", "prefix": prefix}

                    # noinspection PyProtectedMember
                    scan._configuration.soda_cloud = CustomizedSodaClCloud(
                        host=self.soda_cloud.host,
                        api_key_id=self.soda_cloud.api_key_id,
                        api_key_secret=self.soda_cloud.api_key_secret,
                        token=self.soda_cloud.token,
                        port=self.soda_cloud.port,
                        logs=scan_logs,
                        scheme=self.soda_cloud.scheme,
                        default_data_source_properties=default_data_source_properties,
                    )

                if self.variables:
                    scan.add_variables(self.variables)

                scan.add_sodacl_yaml_str(sodacl_yaml_str)
                scan.execute()

        except Exception as e:
            self.logs.error(f"Data contract verification error: {e}", exception=e)

        # The scan warning and error logs are copied into self.logs and at the end of this
        # method, a SodaException is raised if there are error logs.
        self._append_scan_warning_and_error_logs(scan_logs=scan_logs, contract=contract)

        return ContractResult(
            data_source=self.data_source, contract=contract, sodacl_yaml_str=sodacl_yaml_str, logs=self.logs, scan=scan
        )

    def _generate_sodacl_yaml_str(self, contract: Contract) -> str:
        # Serialize the SodaCL YAML object to a YAML string
        sodacl_checks: list = []

        dataset_name: str = QuotingSerializer.quote(contract.dataset_name)
        sodacl_yaml_object: dict = (
            {
                f"filter {dataset_name} [filter]": {"where": contract.filter_sql},
                f"checks for {dataset_name} [filter]": sodacl_checks,
            }
            if contract.filter_sql
            else {f"checks for {dataset_name}": sodacl_checks}
        )

        for check in contract.checks:
            if not check.skip:
                sodacl_check = check.to_sodacl_check()
                if sodacl_check is not None:
                    sodacl_checks.append(sodacl_check)
        yaml_helper: YamlHelper = YamlHelper(logs=self.logs)
        return yaml_helper.write_to_yaml_str(sodacl_yaml_object)

    def _append_scan_warning_and_error_logs(self, scan_logs: soda_core_logs.Logs, contract: Contract) -> None:
        level_map = {
            soda_core_logs.LogLevel.ERROR: LogLevel.ERROR,
            soda_core_logs.LogLevel.WARNING: LogLevel.WARNING,
            soda_core_logs.LogLevel.INFO: LogLevel.INFO,
            soda_core_logs.LogLevel.DEBUG: LogLevel.DEBUG,
        }
        for scan_log in scan_logs.logs:
            if scan_log.level in [soda_core_logs.LogLevel.ERROR, soda_core_logs.LogLevel.WARNING]:
                contracts_location: Location = (
                    Location(
                        file_path=contract.contract_file.get_file_description(),
                        line=scan_log.location.line,
                        column=scan_log.location.col,
                    )
                    if scan_log.location is not None
                    else None
                )
                contracts_level: LogLevel = level_map[scan_log.level]
                self.logs._log(
                    Log(
                        level=contracts_level,
                        message=f"SodaCL: {scan_log.message}",
                        location=contracts_location,
                        exception=scan_log.exception,
                    )
                )


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
        return [f"# Contract results for {contract_result.contract.dataset_name}", str(contract_result)]


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
