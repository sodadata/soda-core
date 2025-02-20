from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from logging import ERROR
from numbers import Number
from typing import Optional

from soda_core.common.logs import Logs, Emoticons
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
        self.logs.debug("Contract verification...")

    def with_contract_yaml_file(self, contract_yaml_file_path: str) -> ContractVerificationBuilder:
        if isinstance(contract_yaml_file_path, str):
            self.logs.debug(f"  ...with contract file path '{contract_yaml_file_path}'")
            self.contract_yaml_sources.append(YamlSource.from_file_path(yaml_file_path=contract_yaml_file_path))
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} ...ignoring invalid contract yaml file '{contract_yaml_file_path}'. "
                f"Expected string, but was {contract_yaml_file_path.__class__.__name__}."
            )
        return self

    def with_contract_yaml_str(self, contract_yaml_str: str) -> ContractVerificationBuilder:
        if isinstance(contract_yaml_str, str):
            self.logs.debug(f"  ...with contract YAML str [{len(contract_yaml_str)}]")
            self.contract_yaml_sources.append(YamlSource.from_str(yaml_str=contract_yaml_str))
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} ...ignoring invalid contract_yaml_str '{contract_yaml_str}'.  "
                f"Expected string, but was {contract_yaml_str.__class__.__name__}"
            )
        return self

    def with_data_source_yaml_file(self, data_source_yaml_file_path: str) -> ContractVerificationBuilder:
        if isinstance(data_source_yaml_file_path, str):
            if self.data_source_yaml_source is None:
                self.logs.debug(f"  ...with data_source_yaml_file_path '{data_source_yaml_file_path}'")
            else:
                self.logs.debug(
                    f"{Emoticons.POLICE_CAR_LIGHT} ...with data_source_yaml_file_path '{data_source_yaml_file_path}'. "
                    f"Ignoring previously configured data source '{self.data_source_yaml_source}'"
                )
            self.data_source_yaml_source = YamlSource.from_file_path(yaml_file_path=data_source_yaml_file_path)
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} ...ignoring invalid data_source_yaml_file_path  '{data_source_yaml_file_path}'.  "
                f"Expected string, but was {data_source_yaml_file_path.__class__.__name__}"
            )
        return self

    def with_data_source_yaml_str(self, data_source_yaml_str: str) -> ContractVerificationBuilder:
        if isinstance(data_source_yaml_str, str):
            if self.data_source_yaml_source is None:
                self.logs.debug(f"  ...with data_source_yaml_str '{data_source_yaml_str}'")
            else:
                self.logs.debug(
                    f"{Emoticons.POLICE_CAR_LIGHT} ...with data_source_yaml_str '{data_source_yaml_str}'. "
                    f"Ignoring previously configured data source '{self.data_source_yaml_source}'"
                )
            self.data_source_yaml_source = YamlSource.from_str(yaml_str=data_source_yaml_str)
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} ...ignoring invalid data_source_yaml_str '{data_source_yaml_str}'. "
                f"Expected string, but was {data_source_yaml_str.__class__.__name__}"
            )
        return self

    def with_data_source(self, data_source: object) -> ContractVerificationBuilder:
        self.logs.debug(f"  ...with provided data_source '{data_source}'")
        self.data_source = data_source
        return self

    def with_soda_cloud_yaml_file(self, soda_cloud_yaml_file_path: str) -> ContractVerificationBuilder:
        if isinstance(soda_cloud_yaml_file_path, str):
            if self.soda_cloud_yaml_source is None:
                self.logs.debug(f"  ...with soda_cloud_yaml_file_path '{soda_cloud_yaml_file_path}'")
            else:
                self.logs.debug(
                    f"{Emoticons.POLICE_CAR_LIGHT} ...with soda_cloud_yaml_file_path '{soda_cloud_yaml_file_path}'. "
                    f"Ignoring previously configured soda cloud '{self.soda_cloud_yaml_source}'"
                )
            self.soda_cloud_yaml_source = YamlSource.from_file_path(yaml_file_path=soda_cloud_yaml_file_path)
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} ...ignoring invalid soda_cloud_yaml_file_path '{soda_cloud_yaml_file_path}'. "
                f"Expected string, but was {soda_cloud_yaml_file_path.__class__.__name__}"
            )
        return self

    def with_soda_cloud_yaml_str(self, soda_cloud_yaml_str: str) -> ContractVerificationBuilder:
        if isinstance(soda_cloud_yaml_str, str):
            if self.soda_cloud_yaml_source is None:
                self.logs.debug(f"  ...with soda_cloud_yaml_str '{soda_cloud_yaml_str}'")
            else:
                self.logs.debug(
                    f"{Emoticons.POLICE_CAR_LIGHT} ...with soda_cloud_yaml_str '{soda_cloud_yaml_str}'. "
                    f"Ignoring previously configured soda cloud '{self.soda_cloud_yaml_source}'"
                )
            self.soda_cloud_yaml_source = YamlSource.from_str(yaml_str=soda_cloud_yaml_str)
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} ...ignoring invalid soda_cloud_yaml_str '{soda_cloud_yaml_str}'. "
                f"Expected string, but was {soda_cloud_yaml_str.__class__.__name__}"
            )
        return self

    def with_soda_cloud(self, soda_cloud: object) -> ContractVerificationBuilder:
        self.logs.debug(f"  ...with provided soda_cloud '{soda_cloud}'")
        self.soda_cloud = soda_cloud
        return self

    def with_variable(self, key: str, value: str) -> ContractVerificationBuilder:
        if isinstance(key, str) and isinstance(value, str):
            self.logs.debug(f"  ...with variable '{key}'")
            self.variables[key] = value
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} ...ignoring invalid variable '{key}'. "
                f"Expected key str and value string"
            )
        return self

    def with_variables(self, variables: dict[str, str]) -> ContractVerificationBuilder:
        if isinstance(variables, dict):
            self.logs.debug(f"  ...with variables {list(variables.keys())}")
            self.variables.update(variables)
        elif variables is None:
            if isinstance(self.variables, dict) and len(self.variables) > 1:
                self.logs.debug(f"  ...removing variables {list(self.variables.keys())} because variables set to None")
            self.variables = None
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} ...ignoring invalid variables '{variables}'. "
                f"Expected dict, but was {variables.__class__.__name__}"
            )
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
            raise SodaException(message=self.get_logs_str(), contract_verification_result=self)
        return self

    def get_logs_str(self) -> str:
        return "/n".join([log.message for log in self.logs.logs])


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
        diagnostics: list[Diagnostic]
    ):
        self.contract: Contract = contract
        self.check: Check = check
        self.metric_value: Number | None = metric_value
        self.outcome: CheckOutcome = outcome
        self.diagnostics: list[Diagnostic] = diagnostics

    def log_summary(self, logs: Logs) -> None:
        outcome_emoticon: str = (
            Emoticons.WHITE_CHECK_MARK if self.outcome == CheckOutcome.PASSED
            else Emoticons.POLICE_CAR_LIGHT if self.outcome == CheckOutcome.FAILED
            else Emoticons.SEE_NO_EVIL
        )
        logs.info(f"{outcome_emoticon} Check {self.outcome.name} {self.check.name}")
        for diagnostic in self.diagnostics:
            logs.info(f"  {diagnostic.log_line()}")


class Measurement:

    def __init__(self, metric_id: str, value: any, metric_name: str | None):
        self.metric_id: str = metric_id
        self.metric_name: str | None = metric_name
        self.value: any = value


@dataclass
class Diagnostic:

    name: str

    @abstractmethod
    def log_line(self) -> str:
        pass


@dataclass
class NumericDiagnostic(Diagnostic):

    value: float

    def log_line(self) -> str:
        return f"Actual {self.name} was {self.value}"


class ContractResult:
    """
    This is the immutable data structure containing all the results from a single contract verification.
    This includes any potential execution errors as well as the results of all the checks performed.
    """

    def __init__(
            self,
            contract: Contract,
            data_source_info: DataSourceInfo,
            data_timestamp: datetime | None,
            started_timestamp: datetime,
            ended_timestamp: datetime,
            measurements: list[Measurement],
            check_results: list[CheckResult],
            logs: Logs
    ):
        self.contract: Contract = contract
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
