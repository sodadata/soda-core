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
from soda_core.common.logs_queue import build_streaming_gatherer
from soda_core.common.scan_context import ScanContext, get_scan_context
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import DataSourceYamlSource, SodaCloudYamlSource

if TYPE_CHECKING:
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


def build_test_connection_log_uploader(
    soda_cloud_file_path: Optional[str],
) -> Optional[Logs]:
    """A ``Logs`` streaming a connection test's records to the scan's Cloud log stream, or None
    when there is no scan id / cloud config. Must be closed to flush the final batch. Connection
    tests need no ``sodaCoreScanStart``: the backend pre-creates their scan in its log-accepting
    state. Public because connection-test commands in soda-extensions reuse it.
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

    return Logs(gatherer=build_streaming_gatherer(soda_cloud, scan_id=scan_id))


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
    scan_definition_name: str,
    include: Optional[list[str]] = None,
    exclude: Optional[list[str]] = None,
    logs: Optional[Logs] = None,
) -> ExitCode:
    """Discover datasets and send the results to Soda Cloud.

    Receives fully resolved dependencies — including the mandatory scan
    definition name (``resolve_scan_definition_name``). Engine failures
    propagate raw: the CLI wiring (``scan.run_scan``) is the single logging
    site and maps them to failure reporting. A rejected results upload is not
    an engine failure: it returns ``RESULTS_NOT_SENT_TO_CLOUD`` directly, so
    no failure report is sent.

    Ingestion goes through the installed ``ScanContext``.
    """
    from soda_core.common.datetime_conversions import resolve_data_timestamp
    from soda_core.discovery.discovery_payload import build_discovery_payload

    soda_logger.info(f"Discovering datasets in data source '{data_source_impl.name}'")

    scan_context: ScanContext = get_scan_context()
    # One dataTimestamp for the whole scan: the start command and the results payload must carry
    # the same value (SODA_SCAN_DATA_TIMESTAMP from the launcher, now otherwise).
    data_timestamp: datetime = resolve_data_timestamp(datetime.now(timezone.utc))
    # Started here, the first point where the scan coordinates are all resolved, so the discovery
    # queries stream their logs on a batched run.
    scan_context.start_scan(
        definition_name=scan_definition_name,
        default_data_source=data_source_impl.name,
        data_timestamp=data_timestamp,
    )

    scan_start_timestamp: datetime = datetime.now(timezone.utc)
    dqns: list[str] = _discover_dqns(data_source_impl, include, exclude)
    scan_end_timestamp: datetime = datetime.now(timezone.utc)

    payload: SodaCoreInsertScanResultsDTO = build_discovery_payload(
        dqns=dqns,
        data_source_name=data_source_impl.name,
        scan_definition_name=scan_definition_name,
        data_timestamp=data_timestamp,
        scan_start_timestamp=scan_start_timestamp,
        scan_end_timestamp=scan_end_timestamp,
        log_records=logs.get_log_records() if logs else None,  # [] once streaming
    )
    if not scan_context.insert_results(payload):
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
