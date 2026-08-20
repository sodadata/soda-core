from __future__ import annotations

from datetime import datetime, timezone
from os.path import dirname, exists
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING, Optional

from soda_core.cli.exit_codes import ExitCode
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import Logs
from soda_core.common.logs_queue import LogsQueue
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import DataSourceYamlSource, SodaCloudYamlSource

if TYPE_CHECKING:
    # batched_scan imports build_streaming_logs from this module: type-only to
    # keep the runtime import acyclic.
    from soda_core.cli.handlers.batched_scan import BatchedScanContext
    from soda_core.common.data_source_impl import DataSourceImpl
    from soda_core.common.soda_cloud_dto import SodaCoreInsertScanResultsDTO


def handle_create_data_source(data_source_file_path: str, data_source_type: str) -> ExitCode:
    soda_logger.info(f"Creating {data_source_type} data source YAML file '{data_source_file_path}'")
    if exists(data_source_file_path):
        soda_logger.error(
            f"Could not create data source file '{data_source_file_path}'. "
            f"File already exists {Emoticons.POLICE_CAR_LIGHT}."
        )
        return ExitCode.LOG_ERRORS
    if data_source_type != "postgres":
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Only type postgres is supported atm")
        return ExitCode.LOG_ERRORS
    dir_name = dirname(data_source_file_path)
    try:
        Path(dir_name).mkdir(parents=True, exist_ok=True)
        with open(data_source_file_path, "w") as text_file:
            text_file.write(
                dedent(
                    """
                type: postgres
                name: postgres_ds
                connection:
                    host: localhost
                    user: ${POSTGRES_USERNAME}
                    password: ${POSTGRES_PASSWORD}
                    database: your_postgres_db
                """
                ).strip()
            )
        soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} Created data source file '{data_source_file_path}'")
        return ExitCode.OK
    except Exception as exc:
        soda_logger.exception(f"An unexpected exception occurred: {exc}")
        return ExitCode.LOG_ERRORS


def handle_test_data_source(
    data_source_file_path: str,
    soda_cloud_file_path: Optional[str] = None,
) -> ExitCode:
    soda_logger.info(f"Testing data source configuration file {data_source_file_path}")
    from soda_core.common.data_source_impl import DataSourceImpl

    # Build the upload Logs before parsing so logs emitted while loading/validating the
    # data source YAML — often the most relevant when a connection test fails early — are
    # captured and streamed to Soda Cloud.
    upload_logs: Optional[Logs] = build_test_connection_log_uploader(
        soda_cloud_file_path=soda_cloud_file_path,
    )

    try:
        data_source_impl: DataSourceImpl = DataSourceImpl.from_yaml_source(
            DataSourceYamlSource.from_file_path(data_source_file_path)
        )
        error_message: Optional[str] = (
            data_source_impl.test_connection_error_message()
            if data_source_impl
            else "Data source could not be created. See logs above. Or re-run with -v"
        )
        if error_message:
            soda_logger.error(
                f"{Emoticons.POLICE_CAR_LIGHT} Could not connect using data source '{data_source_file_path}': "
                f"{error_message}"
            )
            return ExitCode.LOG_ERRORS
        else:
            soda_logger.info(
                f"{Emoticons.WHITE_CHECK_MARK} Success! Connection in '{data_source_file_path}' tested ok."
            )
            return ExitCode.OK
    finally:
        if upload_logs is not None:
            upload_logs.close()


def build_streaming_logs(
    soda_cloud: Optional[SodaCloud],
    stage: str,
    scan_id: Optional[str] = None,
) -> Optional[Logs]:
    """Returns a ``Logs`` backed by a ``LogsQueue`` bound to the scan id, so
    root-logger records stream to the scan's Soda Cloud log stream while the
    command runs — or None when there is no scan id / Cloud client, so callers
    fall back to an in-memory ``Logs``. Must be closed to flush the final batch.

    ``stage`` is a closed backend enum (``CoreStageType``: "main" |
    "diagnosticWarehouse"; unknown values degrade to MAIN server-side).
    """
    if scan_id is None:
        # The scan id is a Cloud-only concept set by the Runner/launcher as SODA_SCAN_ID; it is
        # read from the env helper rather than a CLI argument so the generic CLI stays Cloud-agnostic.
        scan_id = EnvConfigHelper().soda_scan_id
    if not scan_id or soda_cloud is None:
        return None

    logs_queue = LogsQueue(
        soda_cloud=soda_cloud,
        stage=stage,
        scan_id=scan_id,
        dataset="",
    )
    return Logs(gatherer=logs_queue)


def build_test_connection_log_uploader(
    soda_cloud_file_path: Optional[str],
) -> Optional[Logs]:
    """File-path-resolving wrapper around ``build_streaming_logs`` for
    connection tests: resolves the Cloud client from the ``-sc`` YAML, streams
    to the scan's main stage. Returns None when there is no scan id / cloud
    config. Must be closed to flush the final batch.

    Public because connection-test commands outside soda-core (e.g. the
    soda-extensions ``diagnostics-warehouse test`` command) reuse it to stream
    their logs to the same scan-id-keyed endpoint; flows that already hold a
    ``SodaCloud`` call ``build_streaming_logs`` directly.
    """
    scan_id: Optional[str] = EnvConfigHelper().soda_scan_id
    if not scan_id or not soda_cloud_file_path:
        return None

    try:
        soda_cloud = SodaCloud.from_yaml_source(
            SodaCloudYamlSource.from_file_path(soda_cloud_file_path),
            provided_variable_values=None,
        )
    except Exception as e:
        soda_logger.warning(
            f"Could not initialise Soda Cloud log upload for test-connection (scan_id="
            f"{scan_id}): {e}. Continuing without log upload."
        )
        return None

    if soda_cloud is None:
        soda_logger.warning("Soda Cloud configuration could not be parsed; test-connection logs will not be uploaded.")
        return None

    return build_streaming_logs(soda_cloud=soda_cloud, stage="main", scan_id=scan_id)


def _discover_dqns(
    data_source_impl: DataSourceImpl,
    include: Optional[list[str]],
    exclude: Optional[list[str]],
) -> list[str]:
    """Open the connection, discover everything visible and return the DQNs.

    Resolution only parses YAML, so the handler owns the connection lifecycle.
    Query failures propagate raw to the CLI wiring, which logs the traceback.
    """
    from soda_core.discovery.discovery import discover_dataset_dqns

    try:
        data_source_impl.open_connection()
        # Empty prefixes: discover everything visible to the connection.
        return discover_dataset_dqns(
            data_source_impl=data_source_impl,
            prefixes=[],
            include=include,
            exclude=exclude,
        )
    finally:
        data_source_impl.close_connection()


def handle_discover_data_source(
    data_source_impl: DataSourceImpl,
    soda_cloud: SodaCloud,
    scan_definition_name: str,
    include: Optional[list[str]] = None,
    exclude: Optional[list[str]] = None,
    logs: Optional[Logs] = None,
    batched_scan_context: Optional["BatchedScanContext"] = None,
) -> ExitCode:
    """Discover datasets and send the results to Soda Cloud.

    Receives fully resolved dependencies — including the mandatory scan
    definition name (``resolve_scan_definition_name``). Engine failures
    propagate raw: the CLI wiring (``run_batched_scan`` /
    ``dependencies.run_with_failure_reporting``) is the single logging site and
    maps them to failure reporting. A rejected results upload is not an engine
    failure: it returns ``RESULTS_NOT_SENT_TO_CLOUD`` directly, so no failure
    report is sent.

    With a ``batched_scan_context`` the upload routes through
    ``context.insert_results`` (async batch pipeline on managed runs, sync
    fallback otherwise); the payload build is unchanged — a streaming-backed
    ``logs`` yields no records, so the payload's ``logs`` field is empty and
    the stream stays the single log channel.
    """
    from soda_core.discovery.discovery_payload import build_discovery_payload

    soda_logger.info(f"Discovering datasets in data source '{data_source_impl.name}'")

    scan_start_timestamp: datetime = datetime.now(timezone.utc)
    dqns: list[str] = _discover_dqns(data_source_impl, include, exclude)
    scan_end_timestamp: datetime = datetime.now(timezone.utc)

    payload: SodaCoreInsertScanResultsDTO = build_discovery_payload(
        dqns=dqns,
        data_source_name=data_source_impl.name,
        scan_definition_name=scan_definition_name,
        scan_start_timestamp=scan_start_timestamp,
        scan_end_timestamp=scan_end_timestamp,
        log_records=logs.get_log_records() if logs else None,
    )
    accepted: bool = (
        batched_scan_context.insert_results(payload)
        if batched_scan_context is not None
        else soda_cloud.insert_scan_results(payload)
    )
    if not accepted:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Discovery results were not accepted by Soda Cloud.")
        return ExitCode.RESULTS_NOT_SENT_TO_CLOUD

    soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} Discovered {len(dqns)} datasets and sent results to Soda Cloud.")
    return ExitCode.OK


def handle_discover_data_source_locally(
    data_source_impl: DataSourceImpl,
    include: Optional[list[str]] = None,
    exclude: Optional[list[str]] = None,
) -> ExitCode:
    """Discover datasets and print their DQNs to the console.

    Local sibling of ``handle_discover_data_source``: no Soda Cloud, so no
    scan lifecycle and no failure reporting. Failures propagate raw — the CLI
    wiring is the single logging site and maps them to ``LOG_ERRORS``.
    """
    soda_logger.info(f"Discovering datasets in data source '{data_source_impl.name}'")
    dqns: list[str] = _discover_dqns(data_source_impl, include, exclude)

    for dqn in dqns:
        soda_logger.info(dqn)
    soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} Discovered {len(dqns)} datasets (nothing sent to Soda Cloud).")
    return ExitCode.OK
