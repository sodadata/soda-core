from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from logging import ERROR
from numbers import Number
from typing import Optional

from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlSource


class ContractVerificationBuilder:

    def __init__(self):
        self.contract_yaml_sources: list[YamlSource] = []
        self.data_source: Optional['DataSource'] = None
        self.data_source_yaml_source: Optional[YamlSource] = None
        self.soda_cloud: Optional['SodaCloud'] = None
        self.soda_cloud_yaml_source: Optional[YamlSource] = None
        self.variables: dict[str, str] = {}
        self.soda_cloud_skip_publish: bool = False
        self.logs: Logs = Logs()

    def with_contract_yaml_file(self, contract_yaml_file_path: str) -> ContractVerificationBuilder:
        if not isinstance(contract_yaml_file_path, str):
            self.logs.error(message=f"Parameter contract_yaml_file_path must be a string, but was "
                                    f"'{contract_yaml_file_path}' ({contract_yaml_file_path.__class__.__name__})")
        else:
            self.contract_yaml_sources.append(YamlSource.from_file_path(yaml_file_path=contract_yaml_file_path))
        return self

    def with_contract_yaml_str(
        self, contract_yaml_str: str, file_path: str | None = None
    ) -> ContractVerificationBuilder:
        assert isinstance(contract_yaml_str, str)
        self.contract_yaml_sources.append(YamlSource.from_str(yaml_str=contract_yaml_str, file_path=file_path))
        return self

    def with_data_source_yaml_file(self, data_source_yaml_file_path: str) -> ContractVerificationBuilder:
        assert isinstance(data_source_yaml_file_path, str)
        if self.data_source_yaml_source is not None:
            self.logs.error("Duplicate data source definition. Ignoring previous data sources.")
        self.data_source_yaml_source = YamlSource.from_file_path(yaml_file_path=data_source_yaml_file_path)
        return self

    def with_data_source_yaml_str(self, data_source_yaml_str: str) -> ContractVerificationBuilder:
        assert isinstance(data_source_yaml_str, str)
        if self.data_source_yaml_source is not None:
            self.logs.error("Duplicate data source definition. Ignoring previous data sources.")
        self.data_source_yaml_source = YamlSource.from_str(yaml_str=data_source_yaml_str)
        return self

    def with_data_source(self, data_source: object) -> ContractVerificationBuilder:
        self.data_source = data_source
        return self

    def with_soda_cloud_yaml_file(self, soda_cloud_yaml_file_path: str) -> ContractVerificationBuilder:
        assert isinstance(soda_cloud_yaml_file_path, str)
        if self.soda_cloud_yaml_source is not None:
            self.logs.error("Duplicate Soda Cloud definition. Ignoring previous Soda Cloud files.")
        self.soda_cloud_yaml_source = YamlSource.from_file_path(yaml_file_path=soda_cloud_yaml_file_path)
        return self

    def with_soda_cloud_yaml_str(self, soda_cloud_yaml_str: str) -> ContractVerificationBuilder:
        assert isinstance(soda_cloud_yaml_str, str)
        if self.soda_cloud_yaml_source is not None:
            self.logs.error("Duplicate Soda Cloud definition. Ignoring previous Soda Cloud files.")
        self.soda_cloud_yaml_source = YamlSource.from_str(yaml_str=soda_cloud_yaml_str)
        return self

    def with_soda_cloud(self, soda_cloud: object) -> ContractVerificationBuilder:
        self.soda_cloud = soda_cloud
        return self

    def with_variable(self, key: str, value: str) -> ContractVerificationBuilder:
        self.variables[key] = value
        return self

    def with_variables(self, variables: dict[str, str]) -> ContractVerificationBuilder:
        if isinstance(variables, dict):
            self.variables.update(variables)
        return self

    def with_soda_cloud_skip_publish(self) -> ContractVerificationBuilder:
        """
        Skips contract publication on Soda Cloud.
        """
        self.soda_cloud_skip_publish = True
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
            contract_yaml_sources=contract_verification_builder.contract_yaml_sources,
            data_source=contract_verification_builder.data_source,
            data_source_yaml_source=contract_verification_builder.data_source_yaml_source,
            soda_cloud=contract_verification_builder.soda_cloud,
            soda_cloud_yaml_source=contract_verification_builder.soda_cloud_yaml_source,
            variables=contract_verification_builder.variables,
            skip_publish=contract_verification_builder.soda_cloud_skip_publish,
            logs=contract_verification_builder.logs,
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
        return [f"### Contract results for {contract_result.soda_qualified_dataset_name}", str(contract_result)]


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


@dataclass
class YamlFileContentInfo:
    source_content_str: str | None
    local_file_path: str | None
    git_repo: str | None = None # Aspirational, not used yet
    soda_cloud_file_id: str | None = None


@dataclass
class Contract:
    data_source_name: str
    dataset_prefix: list[str]
    dataset_name: str
    soda_qualified_dataset_name: str
    source: YamlFileContentInfo


@dataclass
class DataSourceInfo:
    # TODO rename to DataSource (but first rename DataSource to DataSourceImpl?)
    name: str
    type: str


@dataclass
class Threshold:
    must_be_greater_than: Number | None = None
    must_be_greater_than_or_equal: Number | None = None
    must_be_less_than: Number | None = None
    must_be_less_than_or_equal: Number | None = None


@dataclass
class Check:
    column_name: str | None
    type: str
    name: str # Short description used in UI. Required. Between 1 and 4000 chars.  User defined with key 'name' or auto-generated.
    identity: str
    definition: str
    column_name: str | None
    contract_file_line: int
    contract_file_column: int
    threshold: Threshold | None


class CheckResult(ABC):

    def __init__(
        self,
        contract: Contract,
        check: Check,
        metric_value: Number | None,
        outcome: CheckOutcome,
        diagnostic_lines: list[str]
    ):
        self.contract: Contract = contract
        self.check: Check = check
        self.metric_value: Number | None = metric_value
        self.outcome: CheckOutcome = outcome
        self.diagnostic_lines: list[str] = diagnostic_lines

    def __str__(self) -> str:
        return "\n".join(self.get_log_lines())

    def get_log_lines(self) -> list[str]:
        """
        Provides the summary for the contract result logs, as well as the __str__ impl of this check result.
        Method implementations can use self._get_outcome_line(self)
        """
        log_lines: list[str] = [f"Check {self.outcome.name} {self.check.name}"]
        log_lines.extend([
            f"  {diagnostic_line}" for diagnostic_line in self.diagnostic_lines
        ])
        return log_lines


class Measurement:

    def __init__(self, metric_id: str, value: any, metric_name: str | None):
        self.metric_id: str = metric_id
        self.metric_name: str | None = metric_name
        self.value: any = value


class ContractResult:
    """
    This is the immutable data structure containing all the results from a single contract verification.
    This includes any potential execution errors as well as the results of all the checks performed.
    """

    def __init__(
            self,
            contract_info: Contract,
            data_source_info: DataSourceInfo,
            data_timestamp: datetime | None,
            started_timestamp: datetime,
            ended_timestamp: datetime,
            data_source_name: str,
            soda_qualified_dataset_name: str,
            sql_qualified_dataset_name: str,
            measurements: list[Measurement],
            check_results: list[CheckResult],
            logs: Logs
    ):
        self.contract_info: Contract = contract_info
        # TODO move to contract info or use the data_source_info
        self.data_source_name: str = data_source_name
        # TODO move to contract info
        self.soda_qualified_dataset_name: str = soda_qualified_dataset_name
        # TODO move to contract info
        self.sql_qualified_dataset_name: str = sql_qualified_dataset_name

        self.data_source_info: DataSourceInfo = data_source_info

        self.data_timestamp: datetime | None = data_timestamp
        self.started_timestamp: datetime = started_timestamp
        self.ended_timestamp: datetime = ended_timestamp
        self.measurements: list[Measurement] = measurements
        self.check_results: list[CheckResult] = check_results
        self.logs: Logs = logs

    def failed(self) -> bool:
        """
        Returns true if there are checks that have failed.
        Ignores execution errors in the logs.
        """
        return any(check_result.outcome == CheckOutcome.FAILED for check_result in self.check_results)

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
