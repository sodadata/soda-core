from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from textwrap import indent

from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlFile


class ContractVerificationBuilder:

    def __init__(self):
        self.data_source_yaml_file: YamlFile | None = None
        self.data_source: object | None = None
        self.spark_session: object | None = None
        self.contract_files: list[YamlFile] = []
        self.soda_cloud_file: YamlFile | None = None
        self.plugin_files: list[YamlFile] = []
        self.variables: dict[str, str] = {}
        self.logs: Logs = Logs()

    def with_contract_yaml_file(self, contract_yaml_file_path: str) -> ContractVerificationBuilder:
        if not isinstance(contract_yaml_file_path, str):
            self.logs.error(
                message=f"In ContractVerificationBuilder, parameter contract_yaml_file_path must be "
                        f"a string, but was {contract_yaml_file_path} ({type(contract_yaml_file_path)})"
            )
        self.contract_files.append(YamlFile(
            yaml_file_path=contract_yaml_file_path, logs=self.logs, file_type="contract"
        ))
        return self

    def with_contract_yaml_str(self, contract_yaml_str: str) -> ContractVerificationBuilder:
        assert isinstance(contract_yaml_str, str)
        self.contract_files.append(YamlFile(
            yaml_str=contract_yaml_str, logs=self.logs, file_type="contract")
        )
        return self

    def with_contract_yaml_dict(self, contract_yaml_dict: dict) -> ContractVerificationBuilder:
        assert isinstance(contract_yaml_dict, dict)
        self.contract_files.append(YamlFile(
            yaml_dict=contract_yaml_dict, logs=self.logs, file_type="contract"
        ))
        return self

    def with_data_source_yaml_file(self, data_source_yaml_file_path: str) -> ContractVerificationBuilder:
        assert isinstance(data_source_yaml_file_path, str)
        if self.data_source_yaml_file is not None:
            self.logs.error("Duplicate data source definition. Ignoring previous data sources.")
        self.data_source_yaml_file = YamlFile(
            yaml_file_path=data_source_yaml_file_path, logs=self.logs, file_type="data source"
        )
        return self

    def with_data_source_yaml_str(self, data_source_yaml_str: str) -> ContractVerificationBuilder:
        assert isinstance(data_source_yaml_str, str)
        if self.data_source_yaml_file is not None:
            self.logs.error("Duplicate data source definition. Ignoring previous data sources.")
        self.data_source_yaml_file = YamlFile(logs=self.logs, yaml_str=data_source_yaml_str, file_type="data source")
        return self

    def with_data_source_yaml_dict(self, data_source_yaml_dict: dict) -> ContractVerificationBuilder:
        assert isinstance(data_source_yaml_dict, dict)
        if self.data_source_yaml_file is not None:
            self.logs.error("Duplicate data source definition. Ignoring previous data sources.")
        self.data_source_yaml_file = YamlFile(logs=self.logs, yaml_dict=data_source_yaml_dict, file_type="data source")
        return self

    def with_data_source_spark_session(
        self, spark_session: object, data_source_yaml_dict: dict | None = None
    ) -> ContractVerificationBuilder:
        if data_source_yaml_dict is None:
            data_source_yaml_dict = {}
        assert isinstance(spark_session, object)
        assert isinstance(data_source_yaml_dict, dict)
        data_source_yaml_file = YamlFile(logs=self.logs, yaml_dict=data_source_yaml_dict, file_type="data source")
        self.data_source_yaml_file = data_source_yaml_file
        self.spark_session = spark_session
        return self

    def with_data_source(self, data_source: object) -> ContractVerificationBuilder:
        self.data_source = data_source
        return self

    def with_soda_cloud_yaml_file(self, soda_cloud_yaml_file_path: str) -> ContractVerificationBuilder:
        assert isinstance(soda_cloud_yaml_file_path, str)
        if self.soda_cloud_file is not None:
            self.logs.error("Duplicate Soda Cloud definition. Ignoring previous data sources.")
        self.soda_cloud_file = YamlFile(yaml_file_path=soda_cloud_yaml_file_path, logs=self.logs, file_type="soda cloud")
        return self

    def with_soda_cloud_yaml_str(self, soda_cloud_yaml_str: str) -> ContractVerificationBuilder:
        assert isinstance(soda_cloud_yaml_str, str)
        if self.soda_cloud_file is not None:
            self.logs.error("Duplicate Soda Cloud definition. Ignoring previous data sources.")
        self.soda_cloud_file = YamlFile(yaml_str=soda_cloud_yaml_str, logs=self.logs, file_type="soda cloud")
        return self

    def with_soda_cloud_yaml_dict(self, soda_cloud_yaml_dict: dict) -> ContractVerificationBuilder:
        assert isinstance(soda_cloud_yaml_dict, dict)
        if self.soda_cloud_file is not None:
            self.logs.error("Duplicate Soda Cloud definition. Ignoring previous data sources.")
        self.soda_cloud_file = YamlFile(yaml_dict=soda_cloud_yaml_dict, logs=self.logs, file_type="soda cloud")
        return self

    def with_plugin_yaml_file(self, plugin_yaml_file_path: str) -> ContractVerificationBuilder:
        assert isinstance(plugin_yaml_file_path, str)
        self.plugin_files.append(YamlFile(yaml_file_path=plugin_yaml_file_path, logs=self.logs, file_type="plugin"))
        return self

    def with_plugin_yaml_str(self, plugin_yaml_str: str) -> ContractVerificationBuilder:
        assert isinstance(plugin_yaml_str, str)
        self.plugin_files.append(YamlFile(yaml_str=plugin_yaml_str, logs=self.logs, file_type="plugin"))
        return self

    def with_plugin_yaml_dict(self, plugin_yaml_dict: dict) -> ContractVerificationBuilder:
        assert isinstance(plugin_yaml_dict, dict)
        self.plugin_files.append(YamlFile(yaml_dict=plugin_yaml_dict, logs=self.logs, file_type="plugin"))
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
        from soda_core.contracts.impl.contract_verification_impl import ContractVerificationImpl
        self.contract_verification_impl: ContractVerificationImpl = ContractVerificationImpl(
            data_source_yaml_file=contract_verification_builder.data_source_yaml_file,
            data_source=contract_verification_builder.data_source,
            spark_session=contract_verification_builder.spark_session,
            contract_files=contract_verification_builder.contract_files,
            soda_cloud_file=contract_verification_builder.soda_cloud_file,
            plugin_files=contract_verification_builder.plugin_files,
            variables=contract_verification_builder.variables,
            logs=contract_verification_builder.logs
        )

    def __str__(self) -> str:
        return str(self.contract_verification_impl.logs)

    def execute(self) -> ContractVerificationResult:
        return self.contract_verification_impl.execute()


class ContractVerificationResult:
    def __init__(self, logs: Logs, variables: dict[str, str], contract_results: list[ContractResult]):
        self.logs: Logs = logs
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
        return [f"# Contract results for {contract_result.contract_yaml.dataset_name}", str(contract_result)]


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


class CheckOutcome(Enum):
    PASS = "pass"
    FAIL = "fail"
    NOT_EVALUATED = "not_evaluated"


class CheckResult(ABC):

    def __init__(self, check_summary: str, outcome: CheckOutcome):
        self.check_summary: str = check_summary
        self.outcome: CheckOutcome = outcome

    def __str__(self) -> str:
        return "\n".join(self.get_contract_result_str_lines())

    def get_contract_result_str_lines(self) -> list[str]:
        """
        Provides the summary for the contract result logs, as well as the __str__ impl of this check result.
        Method implementations can use self._get_outcome_line(self)
        """
        return [self.get_outcome_and_summary_line()]

    def get_outcome_and_summary_line(self) -> str:
        return f"Check {self.outcome.name}{self.check_summary}"


class ContractResult:
    """
    This is the immutable data structure containing all the results from a single contract verification.
    This includes any potential execution errors as well as the results of all the checks performed.
    """

    def __init__(
            self,
            contract_yaml: 'ContractYaml',
            check_results: list[CheckResult]
    ):
        self.contract_yaml = contract_yaml
        self.logs: Logs = contract_yaml.contract_yaml_file.logs
        self.check_results: list[CheckResult] = []

    def failed(self) -> bool:
        """
        Returns true if there are checks that have failed.
        Ignores execution errors in the logs.
        """
        return any(check.outcome == CheckOutcome.FAIL for check in self.check_results)

    def passed(self) -> bool:
        """
        Returns true if there are no checks that have failed.
        Ignores execution errors in the logs.
        """
        return not self.failed()

    def __str__(self) -> str:
        error_texts_list: list[str] = [str(error) for error in self.logs.get_errors()]

        check_failure_message_list: list[str] = []

        check_failure_count: int = 0
        for check_result in self.check_results:
            if check_result.outcome == CheckOutcome.FAIL:
                result_str_lines = check_result.get_contract_result_str_lines()
                check_failure_message_list.extend(result_str_lines)
                check_failure_count += 1

        if not error_texts_list and not check_failure_message_list:
            return "All is good. No checks failed. No contract execution errors."

        errors_summary_text = f"{len(error_texts_list)} execution error"
        if len(error_texts_list) != 1:
            errors_summary_text = f"{errors_summary_text}s"

        checks_summary_text = f"{check_failure_count} check failure"
        if check_failure_count != 1:
            checks_summary_text = f"{checks_summary_text}s"

        parts = [f"{checks_summary_text} and {errors_summary_text}"]
        if error_texts_list:
            error_lines_text: str = indent("\n".join(error_texts_list), "  ")
            parts.append(f"Errors: \n{error_lines_text}")

        if check_failure_message_list:
            parts.append("\n".join(check_failure_message_list))

        return "\n".join(parts)
