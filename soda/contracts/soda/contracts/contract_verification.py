from __future__ import annotations

from soda.contracts.contract import ContractResult, Contract
from soda.contracts.impl.contract_verification_impl import FileVerificationDataSource, SparkVerificationDataSource, \
    VerificationDataSource
from soda.contracts.impl.logs import Logs
from soda.contracts.impl.yaml_helper import YamlFile
from soda.contracts.impl.soda_cloud import SodaCloud


class ContractVerification:

    def __init__(self):

        self.logs: Logs = Logs()
        self.variables: dict[str, str] = {}
        self.verification_data_sources: list[VerificationDataSource] = []
        self.verification_data_sources_by_name: dict[str, VerificationDataSource] | None = None
        self.contract_files: list[YamlFile] = []
        self.contracts: list[Contract] = []
        self.contract_results: list[ContractResult] = []
        self.soda_cloud_files: list[YamlFile] = []
        self.soda_cloud: SodaCloud | None = None

    def with_contract_yaml_file(self, contract_yaml_file_path: str) -> ContractVerification:
        assert isinstance(contract_yaml_file_path, str)
        self.contract_files.append(YamlFile(yaml_file_path=contract_yaml_file_path, logs=self.logs))
        return self

    def with_contract_yaml_str(self, contract_yaml_str: str) -> ContractVerification:
        assert isinstance(contract_yaml_str, str)
        self.contract_files.append(YamlFile(yaml_str=contract_yaml_str, logs=self.logs))
        return self

    def with_contract_yaml_dict(self, contract_yaml_dict: dict) -> ContractVerification:
        assert isinstance(contract_yaml_dict, dict)
        self.contract_files.append(YamlFile(yaml_dict=contract_yaml_dict, logs=self.logs))
        return self

    def with_data_source_yaml_file(self, data_sources_yaml_file_path: str) -> ContractVerification:
        assert isinstance(data_sources_yaml_file_path, str)
        data_source_yaml_file = YamlFile(yaml_file_path=data_sources_yaml_file_path, logs=self.logs)
        verification_data_source = FileVerificationDataSource(data_source_yaml_file=data_source_yaml_file)
        self.verification_data_sources.append(verification_data_source)
        return self

    def with_data_source_yaml_str(self, data_sources_yaml_str: str) -> ContractVerification:
        assert isinstance(data_sources_yaml_str, str)
        data_source_yaml_file = YamlFile(logs=self.logs, yaml_str=data_sources_yaml_str)
        verification_data_source = FileVerificationDataSource(data_source_yaml_file=data_source_yaml_file)
        self.verification_data_sources.append(verification_data_source)
        return self

    def with_data_source_yaml_dict(self, data_sources_yaml_dict: dict) -> ContractVerification:
        assert isinstance(data_sources_yaml_dict, dict)
        data_source_yaml_file = YamlFile(logs=self.logs, yaml_dict=data_sources_yaml_dict)
        verification_data_source = FileVerificationDataSource(data_source_yaml_file=data_source_yaml_file)
        self.verification_data_sources.append(verification_data_source)
        return self

    def with_data_source_spark_session(self,
                                       spark_session: object,
                                       data_source_name: str = "spark_ds"
                                       ) -> ContractVerification:
        assert isinstance(spark_session, object) and isinstance(data_source_name, str)
        verification_data_source = SparkVerificationDataSource(spark_session=spark_session, data_source_name=data_source_name)
        self.verification_data_sources.append(verification_data_source)
        return self

    def with_variable(self, key: str, value: str) -> ContractVerification:
        self.variables[key] = value
        return self

    def with_variables(self, variables: dict[str, str]) -> ContractVerification:
        if isinstance(variables, dict):
            self.variables.update(variables)
        return self

    def with_soda_cloud_yaml_file(self, soda_cloud_yaml_file_path: str) -> ContractVerification:
        assert isinstance(soda_cloud_yaml_file_path, str)
        self.soda_cloud_files.append(YamlFile(yaml_file_path=soda_cloud_yaml_file_path, logs=self.logs))
        return self

    def with_soda_cloud_yaml_str(self, soda_cloud_yaml_str: str) -> ContractVerification:
        assert isinstance(soda_cloud_yaml_str, str)
        self.soda_cloud_files.append(YamlFile(yaml_str=soda_cloud_yaml_str, logs=self.logs))
        return self

    def with_soda_cloud_yaml_dict(self, soda_cloud_yaml_dict: dict) -> ContractVerification:
        assert isinstance(soda_cloud_yaml_dict, dict)
        self.soda_cloud_files.append(YamlFile(yaml_dict=soda_cloud_yaml_dict, logs=self.logs))
        return self

    def build(self) -> ContractVerification:
        if self.verification_data_sources_by_name is None:
            self.verification_data_sources_by_name = {}
            # Parse data sources
            undefined_verification_data_source = VerificationDataSource()
            single_verification_data_source: VerificationDataSource | None = None

            for verification_data_source in self.verification_data_sources:
                if verification_data_source.initialize_data_source(self.variables):
                    data_source_name = verification_data_source.data_source.data_source_name
                    self.verification_data_sources_by_name[data_source_name] = verification_data_source

            data_sources_count = len(self.verification_data_sources_by_name)
            if data_sources_count == 1:
                single_verification_data_source = next(iter(self.verification_data_sources_by_name.values()))
            elif data_sources_count == 0:
                single_verification_data_source = undefined_verification_data_source

            # parse the contract files and add them to the matching verification data source
            for contract_file in self.contract_files:
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

        for soda_cloud_file in self.soda_cloud_files:
            if soda_cloud_file.exists():
                soda_cloud_file.parse(self.variables)
                self.soda_cloud = SodaCloud(soda_cloud_file)
                break
        return self

    def execute(self) -> ContractVerification:
        self.build()
        for verification_data_source in self.verification_data_sources_by_name.values():
            contract_results: list[ContractResult] = verification_data_source.ensure_open_and_verify_contracts()
            self.contract_results.extend(contract_results)
        return self

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
            raise SodaException(message=errors_str, contract_verification=self)


class SodaException(Exception):
    """
    See also adr/03_exceptions_vs_error_logs.md
    """

    def __init__(self, message: str | None = None, contract_verification: ContractVerification | None = None):
        self.contract_verification: ContractVerification | None = contract_verification
        message_parts: list[str] = []
        if message:
            message_parts.append(message)
        if self.contract_verification:
            message_parts.append(str(self.contract_verification))
        exception_message: str = "\n".join(message_parts)
        super().__init__(exception_message)
