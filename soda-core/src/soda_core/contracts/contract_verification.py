from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from logging import ERROR, LogRecord
from numbers import Number
from typing import Optional

from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import Logs
from soda_core.common.yaml import (
    ContractYamlSource,
    DataSourceYamlSource,
    SodaCloudYamlSource,
)

logger: logging.Logger = soda_logger


class ContractVerificationSession:
    @classmethod
    def execute(
        cls,
        contract_yaml_sources: list[ContractYamlSource],
        only_validate_without_execute: bool = False,
        variables: Optional[dict[str, str]] = None,
        data_source_impls: Optional[list["DataSourceImpl"]] = None,
        data_source_yaml_sources: Optional[list[DataSourceYamlSource]] = None,
        soda_cloud_impl: Optional["SodaCloud"] = None,
        soda_cloud_yaml_source: Optional[SodaCloudYamlSource] = None,
        soda_cloud_publish_results: bool = False,
        soda_cloud_use_agent: bool = False,
        soda_cloud_use_agent_blocking_timeout_in_minutes: int = 60,
    ) -> ContractVerificationSessionResult:
        from soda_core.contracts.impl.contract_verification_impl import (
            ContractVerificationSessionImpl,
        )

        return ContractVerificationSessionImpl.execute(
            contract_yaml_sources=contract_yaml_sources,
            only_validate_without_execute=only_validate_without_execute,
            variables=variables,
            data_source_impls=data_source_impls,
            data_source_yaml_sources=data_source_yaml_sources,
            soda_cloud_impl=soda_cloud_impl,
            soda_cloud_yaml_source=soda_cloud_yaml_source,
            soda_cloud_publish_results=soda_cloud_publish_results,
            soda_cloud_use_agent=soda_cloud_use_agent,
            soda_cloud_use_agent_blocking_timeout_in_minutes=soda_cloud_use_agent_blocking_timeout_in_minutes,
        )


class ContractVerificationSessionResult:
    def __init__(self, contract_verification_results: list[ContractVerificationResult]):
        self.contract_verification_results: list[ContractVerificationResult] = contract_verification_results

    def get_logs(self) -> list[str]:
        logs: list[str] = []
        for contract_verification_result in self.contract_verification_results:
            logs.extend(contract_verification_result.get_logs())
        return logs

    def get_logs_str(self) -> str:
        return "\n".join(self.get_logs())

    def get_errors(self) -> list[str]:
        errors: list[str] = []
        for contract_verification_result in self.contract_verification_results:
            errors.extend(contract_verification_result.get_errors())
        return errors

    def get_errors_str(self) -> str:
        return "\n".join(self.get_errors())

    def has_errors(self) -> bool:
        return any(
            contract_verification_result.has_errors()
            for contract_verification_result in self.contract_verification_results
        )

    def is_failed(self) -> bool:
        """
        Returns true if there are checks that have failed.
        False is returned if there are no check results.
        Only looks at check results.
        Ignores execution errors in the logs.
        """
        return any(
            contract_verification_result.is_failed()
            for contract_verification_result in self.contract_verification_results
        )

    def is_passed(self) -> bool:
        """
        Returns true if there are no checks that have failed.
        Ignores execution errors in the logs.
        """
        return all(
            contract_verification_result.is_passed()
            for contract_verification_result in self.contract_verification_results
        )

    def is_ok(self) -> bool:
        return all(
            contract_verification_result.is_ok() for contract_verification_result in self.contract_verification_results
        )

    def assert_ok(self) -> ContractVerificationSessionResult:
        if not self.is_ok():
            raise SodaException(message=self.get_errors_str())
        return self


class SodaException(Exception):
    """
    See also adr/03_exceptions_vs_error_logs.md
    """

    def __init__(
        self,
        message: Optional[str] = None,
        contract_verification_result: Optional[ContractVerificationSessionResult] = None,
    ):
        self.contract_verification_result: Optional[ContractVerificationSessionResult] = contract_verification_result
        super().__init__(message)


class CheckOutcome(Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    NOT_EVALUATED = "NOT_EVALUATED"


@dataclass
class YamlFileContentInfo:
    source_content_str: Optional[str]
    local_file_path: Optional[str]
    git_repo: Optional[str] = None  # Aspirational, not used yet
    soda_cloud_file_id: Optional[str] = None


@dataclass
class Contract:
    data_source_name: Optional[str]
    dataset_prefix: list[str]
    dataset_name: str
    soda_qualified_dataset_name: str
    source: YamlFileContentInfo


@dataclass
class DataSource:
    name: str
    type: str


@dataclass
class Threshold:
    must_be_greater_than: Optional[Number] = None
    must_be_greater_than_or_equal: Optional[Number] = None
    must_be_less_than: Optional[Number] = None
    must_be_less_than_or_equal: Optional[Number] = None


@dataclass
class Check:
    column_name: Optional[str]
    type: str
    qualifier: Optional[str]
    name: str  # Short description used in UI. Required. Between 1 and 4000 chars.  User defined with key 'name' or auto-generated.
    identity: str
    definition: str
    column_name: Optional[str]
    contract_file_line: int
    contract_file_column: int
    threshold: Optional[Threshold]


class CheckResult(ABC):
    def __init__(
        self,
        contract: Contract,
        check: Check,
        metric_value: Optional[Number],
        outcome: CheckOutcome,
        diagnostics: list[Diagnostic],
    ):
        self.contract: Contract = contract
        self.check: Check = check
        self.metric_value: Optional[Number] = metric_value
        self.outcome: CheckOutcome = outcome
        self.diagnostics: list[Diagnostic] = diagnostics

    def log_summary(self, logs: Logs) -> None:
        outcome_emoticon: str = (
            Emoticons.WHITE_CHECK_MARK
            if self.outcome == CheckOutcome.PASSED
            else Emoticons.POLICE_CAR_LIGHT
            if self.outcome == CheckOutcome.FAILED
            else Emoticons.SEE_NO_EVIL
        )
        logger.info(f"{outcome_emoticon} Check {self.outcome.name} {self.check.name}")
        for diagnostic in self.diagnostics:
            logger.info(f"  {diagnostic.log_line()}")


class Measurement:
    def __init__(self, metric_id: str, value: any, metric_name: Optional[str]):
        self.metric_id: str = metric_id
        self.metric_name: Optional[str] = metric_name
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


class ContractVerificationResult:
    """
    This is the immutable data structure containing all the results from a single contract verification.
    This includes any potential execution errors as well as the results of all the checks performed.
    """

    def __init__(
        self,
        contract: Contract,
        data_source: DataSource,
        data_timestamp: Optional[datetime],
        started_timestamp: datetime,
        ended_timestamp: datetime,
        measurements: list[Measurement],
        check_results: list[CheckResult],
        sending_results_to_soda_cloud_failed: bool,
        log_records: Optional[list[LogRecord]] = None,
    ):
        self.contract: Contract = contract
        self.data_source: DataSource = data_source

        self.data_timestamp: Optional[datetime] = data_timestamp
        self.started_timestamp: datetime = started_timestamp
        self.ended_timestamp: datetime = ended_timestamp
        self.measurements: list[Measurement] = measurements
        self.check_results: list[CheckResult] = check_results
        self.sending_results_to_soda_cloud_failed: bool = sending_results_to_soda_cloud_failed
        self.log_records: Optional[list[LogRecord]] = log_records

    def get_logs(self) -> list[str]:
        return [r.msg for r in self.log_records]

    def get_logs_str(self) -> str:
        return "\n".join(self.get_logs())

    def get_errors(self) -> list[str]:
        return [r.msg for r in self.log_records if r.levelno >= ERROR]

    def get_errors_str(self) -> str:
        return "\n".join(self.get_errors())

    def has_errors(self) -> bool:
        if self.log_records is None:
            return False
        return any(r.levelno >= ERROR for r in self.log_records)

    def is_failed(self) -> bool:
        """
        Returns true if there are checks that have failed.
        False is returned if there are no check results.
        Only looks at check results.
        Ignores execution errors in the logs.
        """
        return any(check_result.outcome == CheckOutcome.FAILED for check_result in self.check_results)

    def is_passed(self) -> bool:
        """
        Returns true if there are no checks that have failed.
        Ignores execution errors in the logs.
        """
        return not self.is_failed()

    def is_ok(self) -> bool:
        return not self.is_failed() and not self.has_errors()
