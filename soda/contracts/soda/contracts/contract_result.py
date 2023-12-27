from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from textwrap import indent
from typing import List, Dict

import soda.common.logs as soda_common_logs
from soda.contracts.logs import Logs, LogLevel, Log, Location
from soda.scan import Scan

logger = logging.getLogger(__name__)


@dataclass
class CheckDefinition:
    name: str
    sodacl: str
    column: str | None

    @classmethod
    def _from_scan_check(cls, scan_check: Dict[str, object]) -> CheckDefinition:
        return CheckDefinition(
            name=scan_check["name"],
            sodacl=scan_check["definition"],
            column=scan_check.get("column", None)
        )

    def get_console_log_message(self) -> str:
        column_text = f" ({self.column})" if self.column else None
        return f"{self.name}{column_text}"


@dataclass
class Metric:
    name: str
    type: str
    value: object

    @classmethod
    def _from_scan_metric(cls, scan_metric: Dict[str, object]) -> Metric:
        name = scan_metric.get("identity")
        type = scan_metric.get("metricName")
        value = scan_metric.get("value")
        if isinstance(name, str) and isinstance(type, str):
            return Metric(name=name, type=type, value=value)
        else:
            logger.error(f"Invalid metric types name={name} and type={type}")
            return Metric(name=str(name), type=str(type), value=value)

    def get_console_log_message(self) -> str:
        return f"{self.type} was {self.value}"


@dataclass
class CheckDiagnostics:
    metrics: List[Metric]

    @classmethod
    def _from_scan_check(cls, scan_check: Dict[str, object], scan: Scan) -> CheckDiagnostics:
        metrics: List[Metric] = []
        scan_check_metric_identities = scan_check.get("metrics")
        scan_metrics = scan.scan_results.get("metrics")
        if isinstance(scan_check_metric_identities, list) and isinstance(scan_metrics, list):
            for metric_identity in scan_check_metric_identities:
                scan_metric = next(
                    (
                        scan_metric
                        for scan_metric in scan_metrics
                        if scan_metric.get("identity") == metric_identity
                    ),
                    None
                )
                if isinstance(scan_metric, dict):
                    metrics.append(Metric._from_scan_metric(scan_metric))
        return CheckDiagnostics(metrics=metrics)

    def get_console_log_message(self) -> str:
        return "\n".join(metric.get_console_log_message() for metric in self.metrics)


class CheckOutcome(Enum):
    PASS = "pass"
    FAIL = "fail"
    UNKNOWN = "unknown"

    @classmethod
    def _from_scan_check(cls, scan_check: Dict[str, object]) -> CheckOutcome:
        scan_check_outcome = scan_check.get("outcome")
        if scan_check_outcome == "pass":
            return CheckOutcome.PASS
        elif scan_check_outcome == "fail":
            return CheckOutcome.FAIL
        return CheckOutcome.UNKNOWN


@dataclass
class CheckResult:
    outcome: CheckOutcome
    definition: CheckDefinition
    diagnostics: CheckDiagnostics | None

    @classmethod
    def _from_scan_check(cls, scan_check: Dict[str, object], scan: Scan) -> CheckResult:
        outcome: CheckOutcome
        definition: CheckDefinition
        diagnostics: CheckDiagnostics | None
        return CheckResult(
            outcome=CheckOutcome._from_scan_check(scan_check),
            definition=CheckDefinition._from_scan_check(scan_check),
            diagnostics=CheckDiagnostics._from_scan_check(scan_check, scan)
        )

    def get_console_log_message(self) -> str:
        outcome_text = (
            "Check FAILED" if self.outcome == CheckOutcome.FAIL
            else "Check passed" if self.outcome == CheckOutcome.PASS
            else "Check unknown"
        )
        definition_text = indent(self.definition.get_console_log_message(), "  ")
        diagnostics_text = indent(self.diagnostics.get_console_log_message(), "  ")
        return f"{outcome_text}\n{definition_text}\n{diagnostics_text}"


@dataclass
class ContractResult:
    """
    This is the immutable data structure containing all the results from a single contract verification.
    This includes any potential execution errors as well as the results of all the checks performed.
    """

    logs: Logs
    check_results: List[CheckResult]

    @classmethod
    def _from_scan_results(cls, logs: Logs, scan: Scan):
        logs: Logs = Logs(logs)

        level_map = {
            soda_common_logs.LogLevel.ERROR: LogLevel.ERROR,
            soda_common_logs.LogLevel.WARNING: LogLevel.WARNING,
            soda_common_logs.LogLevel.INFO: LogLevel.INFO,
            soda_common_logs.LogLevel.DEBUG: LogLevel.DEBUG
        }
        for scan_log in scan._logs.logs:
            contracts_location: Location = (
                Location(line=scan_log.location.line, column=scan_log.location.col)
                if scan_log.location is not None
                else None
            )
            contracts_level: LogLevel = level_map[scan_log.level]
            logs._log(Log(
                level=contracts_level,
                message=scan_log.message,
                location=contracts_location,
                exception=scan_log.exception
            ))

        check_results: List[CheckResult]  = []
        scan_checks = scan.scan_results.get("checks")
        if isinstance(scan_checks, list):
            for scan_check in scan_checks:
                check_results.append(CheckResult._from_scan_check(scan_check, scan))

        return ContractResult(logs=logs, check_results=check_results)

    def has_problems(self) -> bool:
        return self.logs.has_errors() or self._has_check_failures()

    def assert_no_problems(self) -> None:
        if self.has_problems() or self._has_check_failures():
            raise AssertionError(self.get_problems_text())

    def _has_check_failures(self):
        return any(check.outcome == CheckOutcome.FAIL for check in self.check_results)

    def get_problems_text(self) -> str:
        error_texts_list: List[str] = [
            str(error)
            for error in self.logs.get_errors()
        ]

        check_failure_message_list = [
            check.get_console_log_message()
            for check in self.check_results
            if check.outcome == CheckOutcome.FAIL
        ]

        if not error_texts_list and not check_failure_message_list:
            return "All is good. No errors nor check failures."

        parts = []
        if error_texts_list:
            error_lines_text: str = indent("\n".join(error_texts_list), "  ")
            parts.append(f"Errors: \n{error_lines_text}")

        if check_failure_message_list:
            parts.append("\n".join(check_failure_message_list))

        return "\n".join(parts)
