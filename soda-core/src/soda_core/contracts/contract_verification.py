from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from logging import ERROR, LogRecord
from numbers import Number
from typing import Any, Optional

from soda_core import is_verbose
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import Location
from soda_core.common.yaml import ContractYamlSource, DataSourceYamlSource
from soda_core.contracts.contract_interfaces import ILoggingOutput

logger: logging.Logger = soda_logger


class ContractVerificationSession:
    """Represents the contract verification session.

    Multiple Contracts over multiple Data Sources can be verified in one verification session.

    @param contract_yaml_sources: The list of contract YAML sources to verify.
    @param only_validate_without_execute: If True, only validate the contracts without executing them.
    @param variables: The variables to use in the contract queries.
    @param data_timestamp: The timestamp of the data to use for the verification.
    @param data_source_impls: The data source implementations to use for the verification.
    @param data_source_yaml_sources: The data source YAML sources to use for the verification.
    @param soda_cloud_impl: The Soda Cloud implementation to use for the verification.
    @param soda_cloud_publish_results: If True, publish the results to Soda Cloud.
    @param soda_cloud_use_agent: If True, use the Soda Cloud agent for the verification.
    @param soda_cloud_verbose: If True, enable verbose logging for the Soda Cloud agent.
    """

    @classmethod
    def execute(
        cls,
        contract_yaml_sources: list[ContractYamlSource],
        only_validate_without_execute: bool = False,
        variables: Optional[dict[str, str]] = None,
        data_timestamp: Optional[str] = None,
        data_source_impls: Optional[list["DataSourceImpl"]] = None,
        data_source_yaml_sources: Optional[list[DataSourceYamlSource]] = None,
        soda_cloud_impl: Optional["SodaCloud"] = None,
        soda_cloud_publish_results: bool = False,
        soda_cloud_use_agent: bool = False,
        soda_cloud_verbose: bool = False,
        soda_cloud_use_agent_blocking_timeout_in_minutes: int = 60,
        dwh_data_source_file_path: Optional[str] = None,
    ) -> ContractVerificationSessionResult:
        from soda_core.contracts.impl.contract_verification_impl import (
            ContractVerificationSessionImpl,
        )

        return ContractVerificationSessionImpl.execute(
            contract_yaml_sources=contract_yaml_sources,
            only_validate_without_execute=only_validate_without_execute,
            variables=variables,
            data_timestamp=data_timestamp,
            data_source_impls=data_source_impls,
            data_source_yaml_sources=data_source_yaml_sources,
            soda_cloud_impl=soda_cloud_impl,
            soda_cloud_publish_results=soda_cloud_publish_results,
            soda_cloud_use_agent=soda_cloud_use_agent,
            soda_cloud_verbose=soda_cloud_verbose,
            soda_cloud_use_agent_blocking_timeout_in_minutes=soda_cloud_use_agent_blocking_timeout_in_minutes,
            dwh_data_source_file_path=dwh_data_source_file_path,
        )


class ContractVerificationSessionResult:
    """Represents the result of a contract verification session.

    Provides overview of logs, errors, and the status of the verification process over all of the verified Contracts.

    @param contract_verification_results: The list of contract verification results.
    """

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

    @property
    def number_of_checks(self) -> int:
        return sum(
            contract_verification_result.number_of_checks
            for contract_verification_result in self.contract_verification_results
        )

    @property
    def number_of_checks_passed(self) -> int:
        return sum(
            contract_verification_result.number_of_checks_passed
            for contract_verification_result in self.contract_verification_results
        )

    @property
    def number_of_checks_failed(self) -> int:
        return sum(
            contract_verification_result.number_of_checks_failed
            for contract_verification_result in self.contract_verification_results
        )

    def get_errors_str(self) -> str:
        return "\n".join(self.get_errors())

    @property
    def has_errors(self) -> bool:
        return any(
            contract_verification_result.has_errors
            for contract_verification_result in self.contract_verification_results
        )

    @property
    def is_failed(self) -> bool:
        """
        Returns true if there are checks that have failed.
        False is returned if there are no check results.
        Only looks at check results.
        Ignores execution errors in the logs.
        """
        return any(
            contract_verification_result.is_failed
            for contract_verification_result in self.contract_verification_results
        )

    @property
    def is_passed(self) -> bool:
        """
        Returns true if there are no checks that have failed.
        Ignores execution errors in the logs.
        """
        return all(
            contract_verification_result.is_passed
            for contract_verification_result in self.contract_verification_results
        )

    @property
    def is_ok(self) -> bool:
        return all(
            contract_verification_result.is_ok for contract_verification_result in self.contract_verification_results
        )

    def assert_ok(self) -> ContractVerificationSessionResult:
        if not self.is_ok:
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
    """Represents the state / essence of a check after it has been executed.

    i.e. "after" CheckImpl has been executed and check result is being created.
    """

    column_name: Optional[str]
    type: str
    qualifier: Optional[str]
    name: Optional[str]
    path: str
    identity: str
    definition: str
    column_name: Optional[str]
    contract_file_line: int
    contract_file_column: int
    threshold: Optional[Threshold]
    attributes: Optional[dict[str, Any]]
    location: Optional[Location]


class CheckResult:
    """
    Represents the result of a check.

    @param check: The check that was performed.
    @param outcome: The outcome of the check.
    @param threshold_value: The threshold value that was applied to the check.
    @param diagnostic_metric_values: The diagnostic metric values collected during the check.
    """

    def __init__(
        self,
        check: Check,
        outcome: CheckOutcome,
        threshold_value: Optional[float | int] = None,
        diagnostic_metric_values: Optional[dict[str, float]] = None,
        autogenerate_diagnostics_payload: bool = False,
    ):
        self.check: Check = check
        self.threshold_value: Optional[float | int] = threshold_value
        self.outcome: CheckOutcome = outcome
        self.diagnostic_metric_values: Optional[dict[str, float | int | str]] = diagnostic_metric_values
        self.autogenerate_diagnostics_payload: bool = autogenerate_diagnostics_payload

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
        row["Diagnostics"] = self.log_table_row_diagnostics(verbose=True if is_verbose() else False)

        return row

    def log_table_row_diagnostics(self, verbose: bool = True) -> str:
        diagnostics = []

        if self.diagnostic_metric_values:
            # If the diagnostic metric values implements the ISodaCloudOutput interface, use it
            if isinstance(self.diagnostic_metric_values, ILoggingOutput):
                diagnostics.append(self.diagnostic_metric_values.get_logging_output())
            else:  # Fallback to the default behavior
                for metric_name, value in self.diagnostic_metric_values.items():
                    formatted_value = self._log_console_format(value, verbose=verbose)
                    diagnostics.append(f"{metric_name}: {formatted_value}")
        return "\n".join(diagnostics)

    @classmethod
    def _log_console_format(cls, n: Number | str | list, verbose: bool = True) -> str:
        """
        Couldn't find nicer & simpler code to format:
        * Full number before the comma,
        * At least 2 significant digits after comma
        * Trunc (not round) after 2 significant digits after comma
        """
        try:
            if isinstance(n, str):
                return n
            if isinstance(n, list):
                result = ""
                for element in n:
                    result += cls._log_console_format(element, verbose=verbose) + "\n"
                return result
            # For the person that passes a dict to the function. Feel free to determine the output format :)
            if not verbose:
                return cls.__remove_precision(n)
            else:  # We are in verbose
                return str(n)
        except Exception as e:
            logger.error(f"Error formatting output: {e}")
            return str(n)

    @classmethod
    def __remove_precision(cls, n: Number) -> str:  # Can this be removed and replaced by standard python formatting?
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

    def diagnostics_to_camel_case(self) -> dict[str, Any]:
        """
        Converts diagnostic metric names to camelCase format.
        This is useful for sending diagnostics to Soda Cloud.
        """
        if not self.diagnostic_metric_values:
            return {}

        camel_case_diagnostics = {}
        for key, value in self.diagnostic_metric_values.items():
            camel_case_key = "".join(word.capitalize() if i > 0 else word for i, word in enumerate(key.split("_")))
            camel_case_diagnostics[camel_case_key] = value
        return camel_case_diagnostics


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

    @param contract: The contract that was verified.
    @param data_source: The data source that was used for the verification.
    @param data_timestamp: The timestamp of the data to use for the verification.
    @param ended_timestamp: The timestamp when the verification ended.
    @param status: The status of the verification. One of ContractVerificationStatus.
    @param measurements: The measurements taken during the verification.
    @param check_results: The results of the checks performed during the verification.
    @param sending_results_to_soda_cloud_failed: If True, sending results to Soda Cloud failed.
    @param log_records: The log records generated during the verification.

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

    @property
    def has_errors(self) -> bool:
        return self.status is ContractVerificationStatus.ERROR

    @property
    def is_failed(self) -> bool:
        """
        Returns true if there are checks that have failed.
        False is returned if there are no check results.
        Only looks at check results.
        Ignores execution errors in the logs.
        """
        return self.status is ContractVerificationStatus.FAILED

    @property
    def is_passed(self) -> bool:
        """
        Returns true if there are no checks that have failed.
        Ignores execution errors in the logs.
        """
        return self.status is ContractVerificationStatus.PASSED

    @property
    def is_ok(self) -> bool:
        return not self.is_failed and not self.has_errors

    @property
    def number_of_checks(self) -> int:
        return len(self.check_results)

    @property
    def number_of_checks_passed(self) -> int:
        return len([check_result for check_result in self.check_results if check_result.outcome == CheckOutcome.PASSED])

    @property
    def number_of_checks_failed(self) -> int:
        return len([check_result for check_result in self.check_results if check_result.outcome == CheckOutcome.FAILED])
