"""Shared dependency resolution and failure-mapped execution for CLI flows
that publish results to Soda Cloud.

Handlers receive fully constructed dependencies (``DataSourceImpl``,
``SodaCloud``) instead of file paths. This module owns the parse/create step
and the ONE place where resolution and execution failures map to exit codes
and Soda Cloud failure reporting. Like ``failure_reporting``, it is a stable
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
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import DataSourceYamlSource, SodaCloudYamlSource

if TYPE_CHECKING:
    from soda_core.common.data_source_impl import DataSourceImpl


def resolve_soda_cloud(soda_cloud_file_path: Optional[str]) -> Optional[SodaCloud]:
    """Parse a Soda Cloud configuration file into a ``SodaCloud``.

    Returns None when the configuration is missing or unusable — whether the
    parse returns None or raises (e.g. a missing api key property) — with the
    reason logged as an error.
    """
    if not soda_cloud_file_path:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} A Soda Cloud configuration file (-sc) is required.")
        return None
    try:
        soda_cloud: Optional[SodaCloud] = SodaCloud.from_yaml_source(
            SodaCloudYamlSource.from_file_path(soda_cloud_file_path),
            provided_variable_values=None,
        )
    except Exception as exc:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Soda Cloud configuration could not be parsed: {exc}")
        return None
    if soda_cloud is None:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Soda Cloud configuration could not be parsed.")
    return soda_cloud


def resolve_data_source(data_source_file_path: Optional[str]) -> Optional[DataSourceImpl]:
    """Parse a data source configuration file into a ``DataSourceImpl``.

    Returns None when the data source could not be created — whether the parse
    returns None or raises (unknown type, missing plugin, model validation) —
    with the reason logged. Does not open a connection: the consumer owns the
    connection lifecycle.
    """
    from soda_core.common.data_source_impl import DataSourceImpl

    if not data_source_file_path:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} A data source configuration file (-ds) is required.")
        return None
    try:
        data_source_impl: Optional[DataSourceImpl] = DataSourceImpl.from_yaml_source(
            DataSourceYamlSource.from_file_path(data_source_file_path)
        )
    except Exception as exc:
        # Traceback included, unlike resolve_soda_cloud: cloud parse failures are user
        # config issues, while creation failures can be plugin/import problems where
        # the traceback helps.
        soda_logger.exception(f"{Emoticons.POLICE_CAR_LIGHT} Data source could not be created: {exc}")
        return None
    if data_source_impl is None:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Data source could not be created. See logs above (or -v).")
    return data_source_impl


def run_with_failure_reporting(
    data_source_file_path: Optional[str],
    soda_cloud_file_path: Optional[str],
    command: Callable[[DataSourceImpl, SodaCloud], ExitCode],
) -> ExitCode:
    """Resolve a command's dependencies and run it, mapping every failure to an
    exit code in one place.

    The ``Logs`` collector starts before the first log line so records from the
    resolution steps reach Soda Cloud with a failure report too. This is the
    single logging site for command failures — handlers raise without logging,
    and the two except arms only pick the log form:

    - Unusable Soda Cloud config (resolved first): neither results nor a
      failure report can reach Cloud, so exit ``RESULTS_NOT_SENT_TO_CLOUD``
      and let the managed launcher's fallback mark the scan failed.
    - ``ScanExecutionFailedException``: an expected/validation failure — its
      message is user-facing, logged clean without a traceback.
    - Any other exception: unexpected — logged with the traceback.
    - Both, and a data source resolution failure, report via
      ``report_scan_execution_failure`` with the captured log records (the
      failure line above is logged before the records are captured, so it is
      part of the report).
    - Otherwise the command's own exit code is returned unchanged.
    """
    logs: Logs = Logs()
    soda_cloud: Optional[SodaCloud] = None
    try:
        soda_cloud = resolve_soda_cloud(soda_cloud_file_path)
        if soda_cloud is None:
            return ExitCode.RESULTS_NOT_SENT_TO_CLOUD
        data_source_impl: Optional[DataSourceImpl] = resolve_data_source(data_source_file_path)
        if data_source_impl is None:
            return report_scan_execution_failure(soda_cloud, logs.get_log_records())
        return command(data_source_impl, soda_cloud)
    except ScanExecutionFailedException as exc:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} {exc}")
        return report_scan_execution_failure(soda_cloud, logs.get_log_records())
    except Exception as exc:
        soda_logger.exception(f"Scan execution failed: {exc}")
        return report_scan_execution_failure(soda_cloud, logs.get_log_records())
    finally:
        logs.close()
