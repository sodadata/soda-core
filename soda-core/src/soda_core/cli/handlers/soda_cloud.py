from os.path import dirname, exists
from pathlib import Path
from textwrap import dedent
from typing import Optional

from soda_core.cli.exit_codes import ExitCode
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import Logs
from soda_core.common.yaml import SodaCloudYamlSource


def handle_create_soda_cloud(soda_cloud_file_path: str) -> ExitCode:
    soda_logger.info(f"Creating Soda Cloud YAML file '{soda_cloud_file_path}'")
    if exists(soda_cloud_file_path):
        soda_logger.error(
            f"Could not create soda cloud file '{soda_cloud_file_path}'. "
            f"File already exists {Emoticons.POLICE_CAR_LIGHT}"
        )
        return ExitCode.LOG_ERRORS
    try:
        Path(dirname(soda_cloud_file_path)).mkdir(parents=True, exist_ok=True)
        with open(soda_cloud_file_path, "w") as text_file:
            text_file.write(
                dedent(
                    """
                soda_cloud:
                  host: cloud.soda.io
                  api_key_id: ${SODA_CLOUD_API_KEY_ID}
                  api_key_secret: ${SODA_CLOUD_API_KEY_SECRET}
                """
                ).strip()
            )
        soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} Created Soda Cloud configuration file '{soda_cloud_file_path}'")
        return ExitCode.OK
    except Exception as exc:
        soda_logger.error(f"An unexpected exception occurred: {exc}")
        return ExitCode.LOG_ERRORS


def handle_test_soda_cloud(soda_cloud_file_path: str):
    from soda_core.common.soda_cloud import SodaCloud

    # Start capturing logs
    logs = Logs()

    print(f"Testing soda cloud file {soda_cloud_file_path}")
    soda_cloud_yaml_source: SodaCloudYamlSource = SodaCloudYamlSource.from_file_path(soda_cloud_file_path)
    soda_cloud = SodaCloud.from_yaml_source(soda_cloud_yaml_source, provided_variable_values=None)
    error_msg: Optional[str] = None
    if soda_cloud:
        soda_cloud.test_connection()
        if logs.has_errors():
            error_msg = logs.get_errors_str()
    else:
        error_msg = "Soda Cloud connection could not be created. See logs above. Or re-run with -v"

    if error_msg:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Could not connect to Soda Cloud: {error_msg}")
        return ExitCode.LOG_ERRORS
    else:
        soda_logger.info(
            f"{Emoticons.WHITE_CHECK_MARK} Success! Tested Soda Cloud credentials in '{soda_cloud_file_path}'"
        )
        return ExitCode.OK
