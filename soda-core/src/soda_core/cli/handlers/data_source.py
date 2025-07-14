from os.path import dirname, exists
from pathlib import Path
from textwrap import dedent
from typing import Optional

from soda_core.cli.exit_codes import ExitCode
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.yaml import DataSourceYamlSource


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


def handle_test_data_source(data_source_file_path: str) -> ExitCode:
    soda_logger.info(f"Testing data source configuration file {data_source_file_path}")
    from soda_core.common.data_source_impl import DataSourceImpl

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
        soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} Success! Connection in '{data_source_file_path}' tested ok.")
        return ExitCode.OK
