from __future__ import annotations

from soda.contracts.contract import ContractResult, Contract
from soda.contracts.impl.contract_verification_impl import FileVerificationDataSource, SparkVerificationDataSource, \
    VerificationDataSource
from soda.contracts.impl.logs import Logs
from soda.contracts.impl.yaml_helper import YamlFile
from soda.contracts.impl.soda_cloud import SodaCloud


class ContractVerificationBuilder:

    def __init__(self):
        self.logs: Logs = Logs()
        self.data_source_yaml_files: list[YamlFile] = []
        self.data_source_spark_sessions: dict[str, object] = {}
        self.contract_files: list[YamlFile] = []
        self.soda_cloud_files: list[YamlFile] = []
        self.variables: dict[str, str] = {}

    def with_contract_yaml_file(self, contract_yaml_file_path: str) -> ContractVerificationBuilder:
        if not isinstance(contract_yaml_file_path, str):
            self.logs.error(message=f"In ContractVerificationBuilder, parameter contract_yaml_file_path must be a string, but was {contract_yaml_file_path} ({type(contract_yaml_file_path)})")
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

    def with_data_source_yaml_file(self, data_sources_yaml_file_path: str) -> ContractVerificationBuilder:
        assert isinstance(data_sources_yaml_file_path, str)
        data_source_yaml_file = YamlFile(yaml_file_path=data_sources_yaml_file_path, logs=self.logs)
        self.data_source_yaml_files.append(data_source_yaml_file)
        return self

    def with_data_source_yaml_str(self, data_sources_yaml_str: str) -> ContractVerificationBuilder:
        assert isinstance(data_sources_yaml_str, str)
        data_source_yaml_file = YamlFile(logs=self.logs, yaml_str=data_sources_yaml_str)
        self.data_source_yaml_files.append(data_source_yaml_file)
        return self

    def with_data_source_yaml_dict(self, data_sources_yaml_dict: dict) -> ContractVerificationBuilder:
        assert isinstance(data_sources_yaml_dict, dict)
        data_source_yaml_file = YamlFile(logs=self.logs, yaml_dict=data_sources_yaml_dict)
        self.data_source_yaml_files.append(data_source_yaml_file)
        return self

    def with_data_source_spark_session(self,
                                       spark_session: object,
                                       data_source_name: str = "spark_ds"
                                       ) -> ContractVerificationBuilder:
        assert isinstance(spark_session, object) and isinstance(data_source_name, str)
        self.data_source_spark_sessions[data_source_name] = spark_session
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
        self.verification_data_sources_by_name: dict[str, VerificationDataSource] = {}
        self.contracts: list[Contract] = []
        self.contract_results: list[ContractResult] = []
        self.soda_cloud: SodaCloud | None = None

        self._initialize_verification_data_sources(contract_verification_builder)

        undefined_verification_data_source = VerificationDataSource()
        single_verification_data_source: VerificationDataSource | None = None

        data_sources_count = len(self.verification_data_sources_by_name)
        if data_sources_count == 1:
            single_verification_data_source = next(iter(self.verification_data_sources_by_name.values()))
        elif data_sources_count == 0:
            single_verification_data_source = undefined_verification_data_source

        # parse the contract files and add them to the matching verification data source
        for contract_file in contract_verification_builder.contract_files:
            contract_file.parse(self.variables)
            if contract_file.is_ok():
                contract_data_source_name: str | None = contract_file.dict.get("data_source")
                if isinstance(contract_data_source_name, str):
                    verification_data_source = self.verification_data_sources_by_name.get(contract_data_source_name)
                    if not verification_data_source:
                        self.logs.error(f"Data source '{contract_data_source_name}' not configured")
                        verification_data_source = undefined_verification_data_source
                elif single_verification_data_source:
                    # no data source specified in the contract
                    # and a single data source was specified in the verification
                    verification_data_source = single_verification_data_source
                else:
                    verification_data_source = undefined_verification_data_source

                contract: Contract = Contract.create(
                    data_source=verification_data_source.data_source,
                    contract_file=contract_file,
                    variables=self.variables,
                    soda_cloud=self.soda_cloud,
                    logs=contract_file.logs
                )
                verification_data_source.add_contract(contract)
                self.contracts.append(contract)

        for soda_cloud_file in contract_verification_builder.soda_cloud_files:
            if soda_cloud_file.exists():
                soda_cloud_file.parse(self.variables)
                self.soda_cloud = SodaCloud(soda_cloud_file)
                break

    def _initialize_verification_data_sources(self, contract_verification_builder) -> None:
        # Parse data sources
        for data_source_yaml_file in contract_verification_builder.data_source_yaml_files:
            verification_data_source = FileVerificationDataSource(data_source_yaml_file=data_source_yaml_file)
            if verification_data_source.initialize_data_source(self.variables):
                data_source_name = verification_data_source.data_source.data_source_name
                self.verification_data_sources_by_name[data_source_name] = verification_data_source
        for data_source_name, spark_session in contract_verification_builder.data_source_spark_sessions.items():
            verification_data_source = SparkVerificationDataSource(
                spark_session=spark_session,
                data_source_name=data_source_name
            )
            self.verification_data_sources_by_name[data_source_name] = verification_data_source

    def execute(self) -> ContractVerificationResult:
        all_contract_results: list[ContractResult] = []
        for verification_data_source in self.verification_data_sources_by_name.values():
            all_contract_results.extend(verification_data_source.ensure_open_and_verify_contracts())
        return ContractVerificationResult(
            logs=self.logs,
            variables=self.variables,
            contract_results=all_contract_results
        )

    def __str__(self) -> str:
        blocks: list[str] = [str(self.logs)]
        for contract_result in self.contract_results:
            blocks.extend(self.__format_contract_results_with_heading(contract_result))
        return "\n".join(blocks)


class ContractVerificationResult:
    def __init__(self, logs: Logs, variables: dict[str, str], contract_results: list[ContractResult]):
        self.logs: Logs = logs
        self.variables: dict[str, str] = variables
        self.contract_results: list[ContractResult] = contract_results

    def failed(self):
        """
        Returns True if there are execution errors or if there are check failures.
        """
        return not self.passed()

    def passed(self):
        """
        Returns True if there are no execution errors and no check failures.
        """
        return (
            not self.logs.has_errors()
            and all(contract_result.passed() for contract_result in self.contract_results)
        )

    def has_errors(self):
        return self.logs.has_errors()

    def assert_no_problems(self):
        errors_str: str | None = self.logs.get_errors_str() if self.logs.get_errors() else None
        if errors_str or any(contract_result.failed() for contract_result in self.contract_results):
            raise SodaException(message=errors_str, contract_verification_result=self)

    def __str__(self) -> str:
        blocks: list[str] = [str(self.logs)]
        for contract_result in self.contract_results:
            blocks.extend(self.__format_contract_results_with_heading(contract_result))
        return "\n".join(blocks)

    @classmethod
    def __format_contract_results_with_heading(cls, contract_result: ContractResult) -> list[str]:
        return [
            f"# Contract results for {contract_result.contract.dataset}",
            str(contract_result)
        ]




class SodaException(Exception):
    """
    See also adr/03_exceptions_vs_error_logs.md
    """

    def __init__(self, message: str | None = None, contract_verification_result: ContractVerificationResult | None = None):
        self.contract_verification_result: ContractVerificationResult | None = contract_verification_result
        message_parts: list[str] = []
        if message:
            message_parts.append(message)
        if self.contract_verification_result:
            message_parts.append(str(self.contract_verification_result))
        exception_message: str = "\n".join(message_parts)
        super().__init__(exception_message)
