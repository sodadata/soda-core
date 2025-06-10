from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from logging import ERROR, LogRecord
from numbers import Number
from typing import Optional

from soda_core import is_verbose
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.yaml import ContractYamlSource, DataSourceYamlSource

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
        soda_cloud_publish_results: bool = False,
        soda_cloud_use_agent: bool = False,
        soda_cloud_verbose: bool = False,
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
            soda_cloud_publish_results=soda_cloud_publish_results,
            soda_cloud_use_agent=soda_cloud_use_agent,
            soda_cloud_verbose=soda_cloud_verbose,
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

    def get_number_of_checks(self) -> int:
        return sum(
            contract_verification_result.get_number_of_checks()
            for contract_verification_result in self.contract_verification_results
        )

    def get_number_of_checks_passed(self) -> int:
        return sum(
            contract_verification_result.get_number_of_checks_passed()
            for contract_verification_result in self.contract_verification_results
        )

    def get_number_of_checks_failed(self) -> int:
        return sum(
            contract_verification_result.get_number_of_checks_failed()
            for contract_verification_result in self.contract_verification_results
        )

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


class CheckResult:
    def __init__(
        self,
        contract: Contract,
        check: Check,
        outcome: CheckOutcome,
        threshold_metric_name: Optional[str] = None,
        diagnostic_metric_values: Optional[dict[str, float]] = None,
    ):
        self.contract: Contract = contract
        self.check: Check = check
        self.threshold_metric_name: Optional[str] = threshold_metric_name
        self.outcome: CheckOutcome = outcome
        self.diagnostic_metric_values: Optional[dict[str, float]] = diagnostic_metric_values

    @property
    def outcome_emoticon(self) -> str:
        if self.outcome == CheckOutcome.PASSED:
            return Emoticons.WHITE_CHECK_MARK
        elif self.outcome == CheckOutcome.FAILED:
            return Emoticons.CROSS_MARK
        else:
            return Emoticons.QUESTION_MARK

    @property
    def is_passed(self) -> bool:
        return self.outcome == CheckOutcome.PASSED

    @property
    def is_failed(self) -> bool:
        return self.outcome == CheckOutcome.FAILED

    @property
    def is_not_evaluated(self) -> bool:
        return self.outcome == CheckOutcome.NOT_EVALUATED

    def log_table_row(self) -> dict:
        row = {}
        row["Column"] = self.check.column_name if self.check.column_name else "[dataset-level]"
        row["Check"] = self.check.name
        row["Outcome"] = f"{self.outcome_emoticon} {self.outcome.name}"

        if is_verbose():
            row["Check Type"] = self.check.type
            row["Identity"] = self.check.identity
        row["Details"] = self.log_table_row_diagnostics(verbose=True if is_verbose() else False)

        return row

    def log_table_row_diagnostics(self, verbose: bool = True) -> str:
        diagnostics = []

        if self.diagnostic_metric_values:
            for metric_name, value in self.diagnostic_metric_values.items():
                formatted_value = value
                if not verbose:
                    formatted_value = self._log_console_format(value)
                diagnostics.append(f"{metric_name}: {formatted_value}")
        return "\n".join(diagnostics)

    @classmethod
    def _log_console_format(cls, n: Number) -> str:
        """
        Couldn't find nicer & simpler code to format:
        * Full number before the comma,
        * At least 2 significant digits after comma
        * Trunc (not round) after 2 significant digits after comma
        """
        if n == int(n):
            return str(n)
        n_str = str(n)
        if "e" in n_str:
            n_str = f"{n:.20f}"
        is_index_after_comma = False
        is_after_first_significant_number = False
        significant_numbers_after_comma = 0
        for index in range(0, len(n_str)):
            c = n_str[index]
            if is_index_after_comma and c != "0":
                is_after_first_significant_number = True
            elif not is_index_after_comma and c == ".":
                is_index_after_comma = True
            if is_after_first_significant_number:
                significant_numbers_after_comma += 1
            if significant_numbers_after_comma > 2:
                return n_str[:index]
        return n_str

    def get_threshold_value(self) -> Optional[Number]:
        if self.threshold_metric_name and self.diagnostic_metric_values:
            v = self.diagnostic_metric_values.get(self.threshold_metric_name)
            if isinstance(v, Number):
                return v


class Measurement:
    def __init__(self, metric_id: str, value: any, metric_name: Optional[str]):
        self.metric_id: str = metric_id
        self.metric_name: Optional[str] = metric_name
        self.value: any = value


class ContractVerificationStatus(Enum):
    UNKNOWN = "UNKNOWN"
    FAILED = "FAILED"
    PASSED = "PASSED"
    ERROR = "ERROR"


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
        status: ContractVerificationStatus,
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
        self.status = status

    def get_logs(self) -> list[str]:
        return [r.msg for r in self.log_records]

    def get_logs_str(self) -> str:
        return "\n".join(self.get_logs())

    def get_errors(self) -> list[str]:
        return [r.msg for r in self.log_records if r.levelno >= ERROR]

    def get_errors_str(self) -> str:
        return "\n".join(self.get_errors())

    def has_errors(self) -> bool:
        return self.status is ContractVerificationStatus.ERROR

    def is_failed(self) -> bool:
        """
        Returns true if there are checks that have failed.
        False is returned if there are no check results.
        Only looks at check results.
        Ignores execution errors in the logs.
        """
        return self.status is ContractVerificationStatus.FAILED

    def is_passed(self) -> bool:
        """
        Returns true if there are no checks that have failed.
        Ignores execution errors in the logs.
        """
        return self.status is ContractVerificationStatus.PASSED

    def is_ok(self) -> bool:
        return not self.is_failed() and not self.has_errors()

    def get_number_of_checks(self) -> int:
        return len(self.check_results)

    def get_number_of_checks_passed(self) -> int:
        return len([check_result for check_result in self.check_results if check_result.outcome == CheckOutcome.PASSED])

    def get_number_of_checks_failed(self) -> int:
        return len([check_result for check_result in self.check_results if check_result.outcome == CheckOutcome.FAILED])
