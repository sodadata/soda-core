from __future__ import annotations

from abc import ABC
from enum import Enum
from logging import ERROR

from soda_core.common.data_source import DataSource
from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlSource
from soda_core.contracts.impl.contract_yaml import ContractYaml


class ContractVerificationBuilder:

    def __init__(self, default_data_source: DataSource | None = None):
        self.default_data_source: DataSource | None = default_data_source
        self.contract_yaml_sources: list[YamlSource] = []
        self.variables: dict[str, str] = {}
        self.logs: Logs = Logs()

    def with_contract_yaml_file(self, contract_yaml_file_path: str) -> ContractVerificationBuilder:
        if not isinstance(contract_yaml_file_path, str):
            self.logs.error(message=f"Parameter contract_yaml_file_path must be a string, but was "
                                    f"'{contract_yaml_file_path}' ({contract_yaml_file_path.__class__.__name__})")
        else:
            self.contract_yaml_sources.append(YamlSource.from_file_path(yaml_file_path=contract_yaml_file_path))
        return self

    def with_contract_yaml_str(self, contract_yaml_str: str) -> ContractVerificationBuilder:
        assert isinstance(contract_yaml_str, str)
        self.contract_yaml_sources.append(YamlSource.from_str(yaml_str=contract_yaml_str))
        return self

    def with_contract_yaml_dict(self, contract_yaml_dict: dict) -> ContractVerificationBuilder:
        assert isinstance(contract_yaml_dict, dict)
        self.contract_yaml_sources.append(YamlSource.from_dict(yaml_dict=contract_yaml_dict))
        return self

    # def with_soda_cloud_yaml_file(self, soda_cloud_yaml_file_path: str) -> ContractVerificationBuilder:
    #     assert isinstance(soda_cloud_yaml_file_path, str)
    #     if self.soda_cloud_file is not None:
    #         self.logs.error("Duplicate Soda Cloud definition. Ignoring previous data sources.")
    #     self.soda_cloud_file = YamlSource(yaml_file_path=soda_cloud_yaml_file_path, logs=self.logs, file_type="soda cloud")
    #     return self
    #
    # def with_soda_cloud_yaml_str(self, soda_cloud_yaml_str: str) -> ContractVerificationBuilder:
    #     assert isinstance(soda_cloud_yaml_str, str)
    #     if self.soda_cloud_file is not None:
    #         self.logs.error("Duplicate Soda Cloud definition. Ignoring previous data sources.")
    #     self.soda_cloud_file = YamlSource(yaml_str=soda_cloud_yaml_str, logs=self.logs, file_type="soda cloud")
    #     return self
    #
    # def with_soda_cloud_yaml_dict(self, soda_cloud_yaml_dict: dict) -> ContractVerificationBuilder:
    #     assert isinstance(soda_cloud_yaml_dict, dict)
    #     if self.soda_cloud_file is not None:
    #         self.logs.error("Duplicate Soda Cloud definition. Ignoring previous data sources.")
    #     self.soda_cloud_file = YamlSource(yaml_dict=soda_cloud_yaml_dict, logs=self.logs, file_type="soda cloud")
    #     return self
    #
    # def with_plugin_yaml_file(self, plugin_yaml_file_path: str) -> ContractVerificationBuilder:
    #     assert isinstance(plugin_yaml_file_path, str)
    #     self.plugin_files.append(YamlSource(yaml_file_path=plugin_yaml_file_path, logs=self.logs, file_type="plugin"))
    #     return self
    #
    # def with_plugin_yaml_str(self, plugin_yaml_str: str) -> ContractVerificationBuilder:
    #     assert isinstance(plugin_yaml_str, str)
    #     self.plugin_files.append(YamlSource(yaml_str=plugin_yaml_str, logs=self.logs, file_type="plugin"))
    #     return self
    #
    # def with_plugin_yaml_dict(self, plugin_yaml_dict: dict) -> ContractVerificationBuilder:
    #     assert isinstance(plugin_yaml_dict, dict)
    #     self.plugin_files.append(YamlSource(yaml_dict=plugin_yaml_dict, logs=self.logs, file_type="plugin"))
    #     return self

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
    def builder(cls, default_data_source: DataSource | None = None) -> ContractVerificationBuilder:
        return ContractVerificationBuilder(default_data_source=default_data_source)

    def __init__(self, contract_verification_builder: ContractVerificationBuilder):
        from soda_core.contracts.impl.contract_verification_impl import ContractVerificationImpl
        self.contract_verification_impl: ContractVerificationImpl = ContractVerificationImpl(
            default_data_source=contract_verification_builder.default_data_source,
            contract_yaml_sources=contract_verification_builder.contract_yaml_sources,
            variables=contract_verification_builder.variables,
            logs=contract_verification_builder.logs
        )

    def __str__(self) -> str:
        return str(self.contract_verification_impl.logs)

    def execute(self) -> ContractVerificationResult:
        return self.contract_verification_impl.execute()


class ContractVerificationResult:
    def __init__(self, logs: Logs, contract_results: list[ContractResult]):
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
        has_error: bool = any(log.level >= ERROR for log in self.logs.logs)
        has_check_failures: bool = any(contract_result.failed() for contract_result in self.contract_results)
        if has_error or has_check_failures:
            raise SodaException(message=str(self), contract_verification_result=self)
        return self

    def __str__(self) -> str:
        blocks: list[str] = [str(self.logs)]
        for contract_result in self.contract_results:
            blocks.extend(self.__format_contract_results_with_heading(contract_result))
        return "\n".join(blocks)

    @classmethod
    def __format_contract_results_with_heading(cls, contract_result: ContractResult) -> list[str]:
        return [f"### Contract results for {contract_result.contract_yaml.dataset_name}", str(contract_result)]


class SodaException(Exception):
    """
    See also adr/03_exceptions_vs_error_logs.md
    """

    def __init__(
        self, message: str | None = None, contract_verification_result: ContractVerificationResult | None = None
    ):
        self.contract_verification_result: ContractVerificationResult | None = contract_verification_result
        super().__init__(message)


class CheckOutcome(Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    NOT_EVALUATED = "NOT_EVALUATED"


class CheckResult(ABC):

    def __init__(self, outcome: CheckOutcome, check_summary: str, diagnostic_lines: list[str]):
        self.outcome: CheckOutcome = outcome
        self.check_summary: str = check_summary
        self.diagnostic_lines: list[str] = diagnostic_lines

    def __str__(self) -> str:
        return "\n".join(self.get_log_lines())

    def get_log_lines(self) -> list[str]:
        """
        Provides the summary for the contract result logs, as well as the __str__ impl of this check result.
        Method implementations can use self._get_outcome_line(self)
        """
        log_lines: list[str] = [f"Check {self.outcome.name} {self.check_summary}"]
        log_lines.extend([
            f"  {diagnostic_line}" for diagnostic_line in self.diagnostic_lines
        ])
        return log_lines


class ContractResult:
    """
    This is the immutable data structure containing all the results from a single contract verification.
    This includes any potential execution errors as well as the results of all the checks performed.
    """

    def __init__(
            self,
            contract_yaml: 'ContractYaml',
            check_results: list[CheckResult],
            logs: Logs
    ):
        self.contract_yaml = contract_yaml
        self.logs: Logs = logs
        self.check_results: list[CheckResult] = check_results

    def failed(self) -> bool:
        """
        Returns true if there are checks that have failed.
        Ignores execution errors in the logs.
        """
        return any(check.outcome == CheckOutcome.FAILED for check in self.check_results)

    def passed(self) -> bool:
        """
        Returns true if there are no checks that have failed.
        Ignores execution errors in the logs.
        """
        return not self.failed()

    def __str__(self) -> str:
        log_lines: list[str] = [str(log) for log in self.logs.logs]

        failed_count: int = 0
        not_evaluated_count: int = 0
        passed_count: int = 0
        for check_result in self.check_results:
            result_str_lines = check_result.get_log_lines()
            log_lines.extend(result_str_lines)
            if check_result.outcome == CheckOutcome.FAILED:
                failed_count += 1
            elif check_result.outcome == CheckOutcome.NOT_EVALUATED:
                not_evaluated_count += 1
            elif check_result.outcome == CheckOutcome.PASSED:
                passed_count += 1

        error_count: int = len(self.logs.get_errors())

        not_evaluated_count: int = sum(1 if check_result.outcome == CheckOutcome.NOT_EVALUATED else 0
            for check_result in self.check_results)

        if failed_count + error_count + not_evaluated_count == 0:
            log_lines.append(f"Contract summary: All is good. All {passed_count} checks passed. No execution errors.")
        else:
            log_lines.append(f"Contract summary: Ouch! {failed_count} checks failures, "
                             f"{passed_count} checks passed, {not_evaluated_count} checks not evaluated "
                             f"and {error_count} errors.")

        return "\n".join(log_lines)
