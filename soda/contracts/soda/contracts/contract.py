from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from textwrap import indent
from typing import List, Dict

import soda.common.logs as soda_common_logs
from soda.contracts.connection import Connection
from soda.contracts.impl.logs import Logs, LogLevel, Log, Location
from soda.contracts.impl.variable_resolver import VariableResolver
from soda.contracts.soda_cloud import SodaCloud
from soda.scan import Scan

logger = logging.getLogger(__name__)


class Contract:

    @classmethod
    def from_yaml_str(cls, contract_yaml_str: str) -> Contract:
        """
        Build a contract from a YAML string
        TODO document exceptions vs error response
        """
        return Contract(contract_yaml_str)

    @classmethod
    def from_yaml_file(cls, file_path: str) -> Contract:
        """
        Build a contract from a YAML file.
        TODO document exceptions vs error response
        """
        with open(file_path) as f:
            contract_yaml_str = f.read()
            return Contract(contract_yaml_str)

    def create_updated_contract(self, updates: dict) -> Contract:
        """
        Creates a new contract by applying the updates to this contract.
        Can be used for example to run a contract on a different schema or dataset name.
        """
        raise Exception("TODO")

    def __init__(self, contract_yaml_str: str, soda_cloud: SodaCloud | None = None):
        """
        Consider using Contract.create_from_yaml_str(contract_yaml_str) instead as that is more stable API.
        """
        self.contract_yaml_str: str = contract_yaml_str
        self.soda_cloud: SodaCloud = soda_cloud
        self.sodacl_yaml_str: str | None = None

    def verify(self,
               connection: Connection,
               soda_cloud: SodaCloud | None = None,
               variables: Dict[str, str] | None = None
               ) -> ContractResult:

        """
        Verifies if the data in the dataset matches the contract.
        """

        logs: Logs = Logs()

        scan = Scan()
        try:
            # Resolve all the ${VARIABLES} in the contract based on system variables (os.environ)
            resolved_contract_yaml_str: str = VariableResolver.resolve(self.contract_yaml_str)

            data_contract_translator = connection._create_data_contract_translation(logs)

            # Translate the data contract into SodaCL
            self.sodacl_yaml_str = data_contract_translator.translate_data_contract_yaml_str(
                resolved_contract_yaml_str
            )

            if not logs.has_errors():
                # This assumes the connection is a DataSourceConnection
                data_source = connection.data_source

                # Execute the contract SodaCL in a scan
                scan.set_data_source_name(data_source.data_source_name)
                # noinspection PyProtectedMember
                scan._data_source_manager.data_sources[data_source.data_source_name] = data_source
                scan.add_sodacl_yaml_str(self.sodacl_yaml_str)
                scan.execute()

        except Exception as e:
            logs.error(f"Data contract verification error: {e}", exception=e)

        # noinspection PyProtectedMember
        return ContractResult._from_scan_results(logs, scan)


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

    def assert_no_problems(self) -> None:
        if self.has_problems() or self.has_check_failures():
            raise AssertionError(self.get_problems_text())

    def has_problems(self) -> bool:
        return self.has_execution_errors() or self.has_check_failures()

    def has_execution_errors(self):
        return self.logs.has_errors()

    def has_check_failures(self):
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
