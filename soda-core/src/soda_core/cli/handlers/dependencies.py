"""Shared dependency resolution for CLI flows that publish results to Soda Cloud.

Handlers receive fully constructed dependencies (``DataSourceImpl``,
``SodaCloud``) instead of file paths. Resolvers raise
``ScanExecutionFailedException`` for expected/unusable-configuration shapes;
``scan.run_scan`` owns the failure-to-exit-code mapping for a command and
knows nothing about construction — the wiring layer decides which resolutions
run inside it. Like ``failure_reporting``, this module is a stable import
point: soda-extensions CLIs reuse these utilities for their own
result-publishing commands.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Optional

from soda_core.cli.handlers.failure_reporting import ScanExecutionFailedException
from soda_core.common.exceptions import (
    InvalidDataSourceConfigurationException,
    InvalidSodaCloudConfigurationException,
    YamlParserException,
)
from soda_core.common.logging_constants import soda_logger
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


def resolve_soda_cloud_for_failure_report(
    soda_cloud_file_path: Optional[str], variables: Optional[dict[str, str]] = None
) -> Optional[SodaCloud]:
    """Soda Cloud channel for ``scan.run_scan``, or None when Cloud isn't
    configured or can't be built (``report_scan_execution_failure`` then returns 3 for an
    ad-hoc run, 4 for a managed one).

    The swallow-to-None variant of ``resolve_soda_cloud``, for commands where running
    without Cloud is legal (e.g. a local contract verify). A broken config is not lost:
    the wrapped command re-raises the real config error (it builds its own client from
    the same file) and the boundary reports it through the None channel.
    """
    if not soda_cloud_file_path:
        return None
    try:
        return SodaCloud.from_config(soda_cloud_file_path, variables)
    except Exception:
        # Deliberately broad: this runs outside the failure boundary, where an escaped
        # exception would crash the CLI instead of mapping to a delivery-aware exit code.
        soda_logger.debug(
            f"Could not build the Soda Cloud failure-report channel from '{soda_cloud_file_path}'; "
            f"failures will be reported by exit code only.",
            exc_info=True,
        )
        return None


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


def resolve_scan_definition_name(scan_definition_name: Optional[str]) -> str:
    """Resolve the mandatory scan definition name with precedence: CLI arg > SODA_SCAN_DEFINITION env.

    There is no default: an implicit per-data-source name would silently
    register a new scan definition on Soda Cloud when the configuration is
    missing. When neither source is set this raises
    ``ScanExecutionFailedException`` carrying the user-facing message — call it
    inside the command wrapped by ``scan.run_scan``, which logs the message
    and applies the standard failure mapping (managed scans get marked
    failed).
    """
    resolved_scan_definition_name: Optional[str] = scan_definition_name or os.environ.get("SODA_SCAN_DEFINITION")
    if not resolved_scan_definition_name:
        raise ScanExecutionFailedException(
            "A scan definition name is required to send discovery results to Soda Cloud: "
            "pass --scan-definition-name or set SODA_SCAN_DEFINITION."
        )
    return resolved_scan_definition_name
