from os.path import dirname, exists
from pathlib import Path
from textwrap import dedent
from typing import Optional

from soda_core.cli.exit_codes import ExitCode
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import LogCapturer
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
    scan_reference: Optional[str] = None,
) -> ExitCode:
    soda_logger.info(f"Testing data source configuration file {data_source_file_path}")
    from soda_core.common.data_source_impl import DataSourceImpl

    data_source_impl: DataSourceImpl = DataSourceImpl.from_yaml_source(
        DataSourceYamlSource.from_file_path(data_source_file_path)
    )

    log_uploader: Optional["_TestConnectionLogUploader"] = _build_log_uploader(
        soda_cloud_file_path=soda_cloud_file_path,
        scan_reference=scan_reference,
    )

    try:
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
            soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} Success! Connection in '{data_source_file_path}' tested ok.")
            return ExitCode.OK
    finally:
        if log_uploader is not None:
            log_uploader.close()


class _TestConnectionLogUploader:
    """Wires Soda Cloud log upload around the test-connection flow.

    Holds a LogsQueue bound to the given scan_reference and a LogCapturer that
    forwards root-logger records to it. Must be closed to flush the final batch.
    """

    def __init__(self, logs_queue, log_capturer):
        self._logs_queue = logs_queue
        self._log_capturer = log_capturer

    def close(self) -> None:
        try:
            self._log_capturer.remove_from_root_logger()
        finally:
            self._logs_queue.close()


def _build_log_uploader(
    soda_cloud_file_path: Optional[str],
    scan_reference: Optional[str],
) -> Optional[_TestConnectionLogUploader]:
    if not scan_reference or not soda_cloud_file_path:
        return None

    try:
        soda_cloud = SodaCloud.from_yaml_source(
            SodaCloudYamlSource.from_file_path(soda_cloud_file_path),
            provided_variable_values=None,
        )
    except Exception as e:
        soda_logger.warning(
            f"Could not initialise Soda Cloud log upload for test-connection (scan_reference="
            f"{scan_reference}): {e}. Continuing without log upload."
        )
        return None

    if soda_cloud is None:
        soda_logger.warning(
            "Soda Cloud configuration could not be parsed; test-connection logs will not be uploaded."
        )
        return None

    logs_queue = LogsQueue(
        soda_cloud=soda_cloud,
        stage="test_connection",
        scan_reference=scan_reference,
        dataset="",
    )
    log_capturer = LogCapturer(logs_queue)
    return _TestConnectionLogUploader(logs_queue=logs_queue, log_capturer=log_capturer)
