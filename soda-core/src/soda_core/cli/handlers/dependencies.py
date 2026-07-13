"""Shared dependency resolution and failure-mapped execution for CLI flows
that publish results to Soda Cloud.

Handlers receive fully constructed dependencies (``DataSourceImpl``,
``SodaCloud``) instead of file paths. Resolvers raise
``ScanExecutionFailedException`` for expected/unusable-configuration shapes;
``run_with_failure_reporting`` owns the failure-to-exit-code mapping for a
command and knows nothing about construction — the wiring layer decides which
resolutions run inside it. Like ``failure_reporting``, this module is a stable
import point: soda-extensions CLIs reuse these utilities for their own
result-publishing commands.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.failure_reporting import (
    ScanExecutionFailedException,
    report_scan_execution_failure,
)
from soda_core.common.exceptions import (
    InvalidDataSourceConfigurationException,
    InvalidSodaCloudConfigurationException,
    YamlParserException,
)
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import DataSourceYamlSource, SodaCloudYamlSource

if TYPE_CHECKING:
    from soda_core.common.data_source_impl import DataSourceImpl


def resolve_soda_cloud(soda_cloud_file_path: Optional[str]) -> SodaCloud:
    """Parse a Soda Cloud configuration file into a ``SodaCloud``.

    Raises ``ScanExecutionFailedException`` carrying the user-facing message
    when the configuration is missing or unusable (missing flag, missing or
    syntactically invalid YAML file, parse returning None, invalid or
    validation-rejected config) — nothing is logged here, the caller owns the
    logging. Pydantic ``ValidationError`` is covered via ``ValueError``, its
    base class. Genuinely unexpected failures propagate raw so the caller logs
    them with the traceback.
    """
    if not soda_cloud_file_path:
        raise ScanExecutionFailedException("A Soda Cloud configuration file (-sc) is required.")
    try:
        soda_cloud: Optional[SodaCloud] = SodaCloud.from_yaml_source(
            SodaCloudYamlSource.from_file_path(soda_cloud_file_path),
            provided_variable_values=None,
        )
    except (InvalidSodaCloudConfigurationException, YamlParserException, ValueError) as exc:
        raise ScanExecutionFailedException(f"Soda Cloud configuration could not be parsed: {exc}") from exc
    if soda_cloud is None:
        raise ScanExecutionFailedException("Soda Cloud configuration could not be parsed.")
    return soda_cloud


def resolve_data_source(data_source_file_path: Optional[str]) -> DataSourceImpl:
    """Parse a data source configuration file into a ``DataSourceImpl``.

    Raises ``ScanExecutionFailedException`` carrying the user-facing message
    for expected shapes (missing flag, missing or syntactically invalid YAML
    file, parse returning None, missing 'type', model validation) — nothing is
    logged here, the caller owns the logging. Pydantic ``ValidationError`` is
    covered via ``ValueError``, its base class. Environment problems (e.g.
    ``ImportError`` from a missing plugin) propagate raw so the caller logs
    them with the traceback, which helps there in a way it doesn't for
    user-config mistakes. Does not open a connection: the consumer owns the
    connection lifecycle.
    """
    from soda_core.common.data_source_impl import DataSourceImpl

    if not data_source_file_path:
        raise ScanExecutionFailedException("A data source configuration file (-ds) is required.")
    try:
        data_source_impl: Optional[DataSourceImpl] = DataSourceImpl.from_yaml_source(
            DataSourceYamlSource.from_file_path(data_source_file_path)
        )
    except (InvalidDataSourceConfigurationException, YamlParserException, ValueError) as exc:
        raise ScanExecutionFailedException(f"Data source could not be created: {exc}") from exc
    if data_source_impl is None:
        raise ScanExecutionFailedException("Data source could not be created. See logs above (or -v).")
    return data_source_impl


def run_with_failure_reporting(
    soda_cloud: SodaCloud,
    command: Callable[[], ExitCode],
) -> ExitCode:
    """Run a command with every failure mapped to an exit code and reported to
    Soda Cloud in one place.

    Receives the already-constructed reporting channel (``soda_cloud``);
    dependency construction lives at the wiring layer, which decides what
    resolves inside the command. Owns the ``Logs`` lifecycle — the collector
    starts before the command, so in-command resolution failures are captured
    too. This is the single logging site for command failures; the two except
    arms only pick the log form:

    - ``ScanExecutionFailedException``: an expected/validation failure — its
      user-facing message is logged clean, without a traceback.
    - Any other exception: unexpected — logged with the traceback.

    Both then report via ``report_scan_execution_failure`` with the captured
    log records (the failure line is logged before the records are captured,
    so it is part of the report). Otherwise the command's own exit code is
    returned unchanged.
    """
    logs: Logs = Logs()
    try:
        return command()
    except ScanExecutionFailedException as exc:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} {exc}")
        return report_scan_execution_failure(soda_cloud, logs.get_log_records())
    except Exception as exc:
        soda_logger.exception(f"Scan execution failed: {exc}")
        return report_scan_execution_failure(soda_cloud, logs.get_log_records())
    finally:
        logs.close()
