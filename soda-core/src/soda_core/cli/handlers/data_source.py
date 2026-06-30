from os.path import dirname, exists
from pathlib import Path
from textwrap import dedent
from typing import Optional

from soda_core.cli.exit_codes import ExitCode
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import Logs
from soda_core.common.logs_queue import LogsQueue
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import DataSourceYamlSource, SodaCloudYamlSource


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
    """Returns a ``Logs`` backed by a ``LogsQueue`` bound to the scan id, so
    root-logger records during the test stream to Soda Cloud — or None when
    there is no scan id / cloud config. Must be closed to flush the final batch.

    Public because connection-test commands outside soda-core (e.g. the
    soda-extensions ``diagnostics-warehouse test`` command) reuse it to stream
    their logs to the same scan-id-keyed endpoint.
    """
    # The scan id is a Cloud-only concept set by the Runner/launcher as SODA_SCAN_ID; it is
    # read from the env helper rather than a CLI argument so the generic CLI stays Cloud-agnostic.
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

    logs_queue = LogsQueue(
        soda_cloud=soda_cloud,
        stage="test_connection",
        scan_id=scan_id,
        dataset="",
    )
    return Logs(gatherer=logs_queue)


def handle_discover_data_source(
    data_source_file_path: str,
    include: Optional[list[str]] = None,
    exclude: Optional[list[str]] = None,
    scan_definition_name: Optional[str] = None,
    soda_cloud_file_path: Optional[str] = None,
) -> ExitCode:
    from soda_core.common.data_source_impl import DataSourceImpl
    from soda_core.discovery.discovery_payload import build_discovery_payload, send_discovery_results
    from soda_core.discovery.discovery_run import DiscoveryRun

    soda_logger.info(f"Discovering datasets for data source configuration file {data_source_file_path}")
    data_source_impl: Optional[DataSourceImpl] = DataSourceImpl.from_yaml_source(
        DataSourceYamlSource.from_file_path(data_source_file_path)
    )
    if data_source_impl is None:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Data source could not be created. See logs above (or -v).")
        return ExitCode.LOG_ERRORS

    if not soda_cloud_file_path:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Discovery requires a Soda Cloud configuration (-sc).")
        return ExitCode.LOG_ERRORS
    soda_cloud: Optional[SodaCloud] = SodaCloud.from_yaml_source(
        SodaCloudYamlSource.from_file_path(soda_cloud_file_path),
        provided_variable_values=None,
    )
    if soda_cloud is None:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Soda Cloud configuration could not be parsed.")
        return ExitCode.LOG_ERRORS

    try:
        # Scope: discover everything visible to the connection (v3 behaviour); include/exclude narrow it.
        dqns: list[str] = DiscoveryRun.execute(
            data_source_impl=data_source_impl,
            prefixes=[],
            include=include,
            exclude=exclude,
        )
    except Exception as exc:
        soda_logger.exception(f"Discovery query failed: {exc}")
        return ExitCode.LOG_ERRORS

    resolved_scan_definition_name: str = scan_definition_name or f"{data_source_impl.name}_schema_discovery_scan"
    payload: dict = build_discovery_payload(
        dqns=dqns,
        data_source_name=data_source_impl.name,
        scan_definition_name=resolved_scan_definition_name,
    )
    response = send_discovery_results(soda_cloud, payload)
    if response is None or not response.ok:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Discovery results were not accepted by Soda Cloud.")
        return ExitCode.RESULTS_NOT_SENT_TO_CLOUD

    soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} Discovered {len(dqns)} datasets and sent results to Soda Cloud.")
    return ExitCode.OK
