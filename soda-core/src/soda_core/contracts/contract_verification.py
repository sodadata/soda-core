from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from logging import LogRecord, ERROR
from numbers import Number
from typing import Optional

from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlSource

logger: logging.Logger = soda_logger


class ContractVerificationSessionBuilder:
    def __init__(self, logs: Optional[Logs] = None):
        self.contract_yaml_sources: list[YamlSource] = []
        self.data_source_impl: Optional["DataSourceImpl"] = None
        self.data_source_yaml_source: Optional[YamlSource] = None
        self.soda_cloud: Optional["SodaCloud"] = None
        self.soda_cloud_yaml_source: Optional[YamlSource] = None
        self.variables: dict[str, str] = {}
        self.soda_cloud_skip_publish: bool = False
        self.use_agent: bool = False
        self.blocking_timeout_in_minutes: Optional[int] = None
        self.logs: Logs = logs if logs else Logs()
        logger.debug("Contract verification...")

    def with_contract_yaml_file(self, contract_yaml_file_path: str) -> ContractVerificationSessionBuilder:
        if isinstance(contract_yaml_file_path, str):
            logger.debug(f"  ...with contract file path '{contract_yaml_file_path}'")
            self.contract_yaml_sources.append(YamlSource.from_file_path(yaml_file_path=contract_yaml_file_path))
        else:
            logger.error(
                f"Ignoring invalid contract yaml file '{contract_yaml_file_path}'. "
                f"Expected string, but was {contract_yaml_file_path.__class__.__name__}."
            )
        return self

    def with_contract_yaml_str(self, contract_yaml_str: str) -> ContractVerificationSessionBuilder:
        if isinstance(contract_yaml_str, str):
            logger.debug(f"  ...with contract YAML str [{len(contract_yaml_str)}]")
            self.contract_yaml_sources.append(YamlSource.from_str(yaml_str=contract_yaml_str))
        else:
            logger.error(
                f"Ignoring invalid contract_yaml_str '{contract_yaml_str}'.  "
                f"Expected string, but was {contract_yaml_str.__class__.__name__}"
            )
        return self

    def with_data_source_yaml_file(self, data_source_yaml_file_path: str) -> ContractVerificationSessionBuilder:
        if isinstance(data_source_yaml_file_path, str):
            if self.data_source_yaml_source is None:
                logger.debug(f"  ...with data_source_yaml_file_path '{data_source_yaml_file_path}'")
            else:
                logger.debug(
                    f"  ...with data_source_yaml_file_path '{data_source_yaml_file_path}'. "
                    f"(Ignoring previously configured data source '{self.data_source_yaml_source}')"
                )
            self.data_source_yaml_source = YamlSource.from_file_path(yaml_file_path=data_source_yaml_file_path)
        else:
            logger.error(
                f"Ignoring invalid data_source_yaml_file_path  '{data_source_yaml_file_path}'.  "
                f"Expected string, but was {data_source_yaml_file_path.__class__.__name__}"
            )
        return self

    def with_data_source_yaml_str(self, data_source_yaml_str: str) -> ContractVerificationSessionBuilder:
        if isinstance(data_source_yaml_str, str):
            if self.data_source_yaml_source is None:
                logger.debug(f"  ...with data_source_yaml_str '{data_source_yaml_str}'")
            else:
                logger.debug(
                    f"  ...with data_source_yaml_str '{data_source_yaml_str}'. "
                    f"(Ignoring previously configured data source '{self.data_source_yaml_source}')"
                )
            self.data_source_yaml_source = YamlSource.from_str(yaml_str=data_source_yaml_str)
        else:
            logger.error(
                f"Ignoring invalid data_source_yaml_str '{data_source_yaml_str}'. "
                f"Expected string, but was {data_source_yaml_str.__class__.__name__}"
            )
        return self

    def with_data_source(self, data_source_impl: "DataSourceImpl") -> ContractVerificationSessionBuilder:
        logger.debug(f"  ...with provided data_source '{data_source_impl}'")
        self.data_source_impl = data_source_impl
        return self

    def with_soda_cloud_yaml_file(self, soda_cloud_yaml_file_path: str) -> ContractVerificationSessionBuilder:
        if isinstance(soda_cloud_yaml_file_path, str):
            if self.soda_cloud_yaml_source is None:
                logger.debug(f"  ...with soda_cloud_yaml_file_path '{soda_cloud_yaml_file_path}'")
            else:
                logger.debug(
                    f"  ...with soda_cloud_yaml_file_path '{soda_cloud_yaml_file_path}'. "
                    f"(Ignoring previously configured soda cloud '{self.soda_cloud_yaml_source}')"
                )
            self.soda_cloud_yaml_source = YamlSource.from_file_path(yaml_file_path=soda_cloud_yaml_file_path)
        else:
            logger.error(
                f"Ignoring invalid soda_cloud_yaml_file_path '{soda_cloud_yaml_file_path}'. "
                f"Expected string, but was {soda_cloud_yaml_file_path.__class__.__name__}"
            )
        return self

    def with_soda_cloud_yaml_str(self, soda_cloud_yaml_str: str) -> ContractVerificationSessionBuilder:
        if isinstance(soda_cloud_yaml_str, str):
            if self.soda_cloud_yaml_source is None:
                logger.debug(f"  ...with soda_cloud_yaml_str [{len(soda_cloud_yaml_str)}]")
            else:
                logger.debug(
                    f"  ...with soda_cloud_yaml_str '{soda_cloud_yaml_str}'. "
                    f"(Ignoring previously configured soda cloud '{self.soda_cloud_yaml_source}')"
                )
            self.soda_cloud_yaml_source = YamlSource.from_str(yaml_str=soda_cloud_yaml_str)
        else:
            logger.error(
                f"Ignoring invalid soda_cloud_yaml_str '{soda_cloud_yaml_str}'. "
                f"Expected string, but was {soda_cloud_yaml_str.__class__.__name__}"
            )
        return self

    def with_soda_cloud(self, soda_cloud: object) -> ContractVerificationSessionBuilder:
        logger.debug(f"  ...with provided soda_cloud '{soda_cloud}'")
        self.soda_cloud = soda_cloud
        return self

    def with_execution_on_soda_agent(
        self, blocking_timeout_in_minutes: Optional[int] = None
    ) -> ContractVerificationSessionBuilder:
        logger.debug(f"  ...with execution on Soda Agent")
        self.use_agent = True
        self.blocking_timeout_in_minutes = blocking_timeout_in_minutes
        return self

    def with_variable(self, key: str, value: str) -> ContractVerificationSessionBuilder:
        if isinstance(key, str) and isinstance(value, str):
            logger.debug(f"  ...with variable '{key}'")
            self.variables[key] = value
        else:
            logger.error(f"Ignoring invalid variable '{key}'. " f"Expected key str and value string")
        return self

    def with_variables(self, variables: dict[str, str]) -> ContractVerificationSessionBuilder:
        if isinstance(variables, dict):
            logger.debug(f"  ...with variables {list(variables.keys())}")
            self.variables.update(variables)
        elif variables is None:
            if isinstance(self.variables, dict) and len(self.variables) > 1:
                logger.debug(f"  ...removing variables {list(self.variables.keys())} because variables set to None")
            self.variables = None
        else:
            logger.error(
                f"Ignoring invalid variables '{variables}'. " f"Expected dict, but was {variables.__class__.__name__}"
            )
        return self

    def with_soda_cloud_skip_publish(self) -> ContractVerificationSessionBuilder:
        """
        Skips contract publication on Soda Cloud.
        """
        self.soda_cloud_skip_publish = True
        return self

    def build(self) -> ContractVerificationSession:
        return ContractVerificationSession(contract_verification_session_builder=self)

    def execute(self) -> ContractVerificationSessionResult:
        contract_verification_session: ContractVerificationSession = self.build()
        return contract_verification_session.execute()


class ContractVerificationSession:
    @classmethod
    def builder(cls, logs: Optional[Logs] = None) -> ContractVerificationSessionBuilder:
        return ContractVerificationSessionBuilder(logs)

    @classmethod
    def execute(
        cls,
        contract_yaml_sources: list[YamlSource],
        only_validate_without_execute: bool = False,
        variables: Optional[dict[str, str]] = None,
        data_source_impls: Optional[list["DataSourceImpl"]] = None,
        data_source_yaml_sources: Optional[list[YamlSource]] = None,
        soda_cloud_impl: Optional["SodaCloud"] = None,
        soda_cloud_yaml_source: Optional[YamlSource] = None,
        soda_cloud_skip_publish: bool = False,
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
            soda_cloud_skip_publish=soda_cloud_skip_publish,
            soda_cloud_use_agent=soda_cloud_use_agent,
            soda_cloud_use_agent_blocking_timeout_in_minutes=soda_cloud_use_agent_blocking_timeout_in_minutes
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
            contract_verification_result.is_ok()
            for contract_verification_result in self.contract_verification_results
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
        log_records: list[LogRecord],
    ):
        self.contract: Contract = contract
        self.data_source: DataSource = data_source

        self.data_timestamp: Optional[datetime] = data_timestamp
        self.started_timestamp: datetime = started_timestamp
        self.ended_timestamp: datetime = ended_timestamp
        self.measurements: list[Measurement] = measurements
        self.check_results: list[CheckResult] = check_results
        self.sending_results_to_soda_cloud_failed: bool = sending_results_to_soda_cloud_failed
        self.log_records: list[LogRecord] = log_records

    def get_logs(self) -> list[str]:
        return [r.msg for r in self.log_records]

    def get_logs_str(self) -> str:
        return "\n".join(self.get_logs())

    def get_errors(self) -> list[str]:
        return [r.msg for r in self.log_records if r.levelno >= ERROR]

    def get_errors_str(self) -> str:
        return "\n".join(self.get_errors())

    def has_errors(self) -> bool:
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
