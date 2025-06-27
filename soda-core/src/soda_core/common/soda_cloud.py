from __future__ import annotations

import base64
import json
import logging
import os
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from logging import LogRecord
from tempfile import TemporaryFile
from time import sleep
from typing import Any, Optional

import requests
from requests import Response
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.datetime_conversions import (
    convert_datetime_to_str,
    convert_str_to_datetime,
)
from soda_core.common.exceptions import (
    ContractNotFoundException,
    DatasetNotFoundException,
    DataSourceNotFoundException,
    InvalidSodaCloudConfigurationException,
    SodaCloudAuthenticationFailedException,
    SodaCloudException,
)
from soda_core.common.logging_constants import Emoticons, ExtraKeys, soda_logger
from soda_core.common.logs import Location, Logs
from soda_core.common.soda_cloud_dto import CheckAttribute, CheckAttributes
from soda_core.common.statements.metadata_columns_query import ColumnMetadata
from soda_core.common.version import SODA_CORE_VERSION, clean_soda_core_version
from soda_core.common.yaml import SodaCloudYamlSource, YamlObject
from soda_core.contracts.contract_publication import ContractPublicationResult
from soda_core.contracts.contract_verification import (
    Check,
    CheckOutcome,
    CheckResult,
    Contract,
    ContractVerificationResult,
    ContractVerificationStatus,
    SodaException,
    Threshold,
    YamlFileContentInfo,
)
from soda_core.contracts.impl.contract_yaml import ContractYaml

logger: logging.Logger = soda_logger


class RemoteScanStatus(Enum):
    QUEUING = ("queuing", False)
    EXECUTING = ("executing", False)
    CANCELATION_REQUESTED = ("cancelationRequested", False)
    TIME_OUT_REQUESTED = ("timeOutRequested", False)
    CANCELED = ("canceled", True)
    TIMED_OUT = ("timedOut", True)
    FAILED = ("failed", True)
    COMPLETED_WITH_ERRORS = ("completedWithErrors", True)
    COMPLETED_WITH_FAILURES = ("completedWithFailures", True)
    COMPLETED_WITH_WARNINGS = ("completedWithWarnings", True)
    COMPLETED = ("completed", True)

    def __init__(self, value: str, is_final_state: bool):
        self.value_ = value
        self.is_final_state = is_final_state

    @classmethod
    def from_value(cls, value: str) -> RemoteScanStatus:
        for status in cls:
            if status.value_ == value:
                return status
        raise ValueError(f"Unknown RemoteScanStatus value: {value}")


class TimestampToCreatedLoggingFilter(logging.Filter):
    # The log record created timestamp cannot be passed into the constructor.
    # It has to be updated after it's creation.
    # This is used when logging log records that come from Soda Cloud
    TIMESTAMP: str = "timestamp"

    def filter(self, record):
        if hasattr(record, self.TIMESTAMP):
            record.created = record.timestamp
        return True


class SodaCloud:
    # Constants
    ORG_CONFIG_KEY_DISABLE_COLLECTING_WH_DATA = "disableCollectingWarehouseData"
    CSV_TEXT_MAX_LENGTH = 1500

    @classmethod
    def from_config(
        cls, config_file_path: Optional[str], provided_variable_values: Optional[dict[str, str]] = None
    ) -> Optional[SodaCloud]:
        if not config_file_path:
            return None

        soda_cloud_yaml_source: SodaCloudYamlSource = SodaCloudYamlSource.from_file_path(file_path=config_file_path)
        soda_cloud_yaml_source.resolve(variables=provided_variable_values)
        soda_cloud_yaml_root_object: YamlObject = soda_cloud_yaml_source.parse()

        if not soda_cloud_yaml_root_object:
            logger.error("Invalid Soda Cloud config file: No valid YAML object as file content")
            return None

        soda_cloud_yaml_object: Optional[YamlObject] = soda_cloud_yaml_root_object.read_object_opt("soda_cloud")
        if not soda_cloud_yaml_object:
            logger.debug("key 'soda_cloud' is required in a Soda Cloud configuration file.")

        if soda_cloud_token := os.environ.get("SODA_CLOUD_TOKEN"):
            logger.debug("Found an authentication token in environment variables, ignoring API key authentication.")

        if not soda_cloud_token and not soda_cloud_yaml_object.has_key("api_key_id"):
            raise InvalidSodaCloudConfigurationException(
                f"Missing required 'api_key_id' property in your Soda Cloud configuration."
            )

        if not soda_cloud_token and not soda_cloud_yaml_object.has_key("api_key_secret"):
            raise InvalidSodaCloudConfigurationException(
                f"Missing required 'api_key_secret' property in your Soda Cloud configuration."
            )

        return SodaCloud(
            host=soda_cloud_yaml_object.read_string_opt(key="host", default_value="cloud.soda.io"),
            api_key_id=soda_cloud_yaml_object.read_string(key="api_key_id", required=False),
            api_key_secret=soda_cloud_yaml_object.read_string(key="api_key_secret", required=False),
            token=soda_cloud_token,
            port=soda_cloud_yaml_object.read_string_opt(key="port"),
            scheme=soda_cloud_yaml_object.read_string_opt(key="scheme", default_value="https"),
        )

    @classmethod
    def from_yaml_source(
        cls, soda_cloud_yaml_source: SodaCloudYamlSource, provided_variable_values: Optional[dict[str, str]]
    ) -> Optional[SodaCloud]:
        soda_cloud_yaml_source.resolve(variables=provided_variable_values)
        soda_cloud_yaml_root_object: YamlObject = soda_cloud_yaml_source.parse()

        if not soda_cloud_yaml_root_object:
            logger.error("Invalid Soda Cloud config file: No valid YAML object as file content")
            return None

        soda_cloud_yaml_object: Optional[YamlObject] = soda_cloud_yaml_root_object.read_object_opt("soda_cloud")
        if not soda_cloud_yaml_object:
            logger.debug("key 'soda_cloud' is required in a Soda Cloud configuration file.")

        if soda_cloud_token := os.environ.get("SODA_CLOUD_TOKEN"):
            logger.debug("Found an authentication token in environment variables, ignoring API key authentication.")

        if not soda_cloud_token and not soda_cloud_yaml_object.has_key("api_key_id"):
            raise InvalidSodaCloudConfigurationException(
                f"Missing required 'api_key_id' property in your Soda Cloud configuration."
            )

        if not soda_cloud_token and not soda_cloud_yaml_object.has_key("api_key_secret"):
            raise InvalidSodaCloudConfigurationException(
                f"Missing required 'api_key_secret' property in your Soda Cloud configuration."
            )

        return SodaCloud(
            host=soda_cloud_yaml_object.read_string_opt(key="host", default_value="cloud.soda.io"),
            api_key_id=soda_cloud_yaml_object.read_string(key="api_key_id", required=False),
            api_key_secret=soda_cloud_yaml_object.read_string(key="api_key_secret", required=False),
            token=soda_cloud_token,
            port=soda_cloud_yaml_object.read_string_opt(key="port"),
            scheme=soda_cloud_yaml_object.read_string_opt(key="scheme", default_value="https"),
        )

    def __init__(
        self,
        host: str,
        api_key_id: str,
        api_key_secret: str,
        token: Optional[str],
        port: Optional[str],
        scheme: Optional[str],
    ):
        self.host = host
        self.port = f":{port}" if port else ""
        self.scheme = scheme if scheme else "https"
        self.api_url = f"{self.scheme}://{self.host}{self.port}/api"
        self.api_key_id = api_key_id
        self.api_key_secret = api_key_secret
        self.token: Optional[str] = token
        self.headers = {"User-Agent": f"SodaCore/{SODA_CORE_VERSION}"}
        self.soda_cloud_trace_ids = {}
        self._organization_configuration = None

    def mark_scan_as_failed(
        self, scan_id: Optional[str] = None, logs: Optional[list[LogRecord]] = None, exc: Optional[Exception] = None
    ) -> None:
        """
        Marks a scan as failed in Soda Cloud. This is used when the scan fails before we have any results to send.
        """
        if not scan_id:
            if "SODA_SCAN_ID" not in os.environ:
                logger.debug("No scan ID provided, not marking scan as failed")
                return
            scan_id = os.environ["SODA_SCAN_ID"]

        cloud_log_dicts = (
            [_build_log_cloud_json_dict(log_record, index) for index, log_record in enumerate(logs)] if logs else []
        )

        if exc:
            cloud_log_dicts = _append_exception_to_cloud_log_dicts(cloud_log_dicts, exc)

        self._execute_command(
            command_json_dict={"type": "sodaCoreMarkScanFailed", "scanId": scan_id, "logs": cloud_log_dicts},
            request_log_name="mark_scan_as_failed",
        )

    def upload_contract_file(self, contract_yaml_source_str: str, soda_cloud_file_path: str) -> Optional[str]:
        logger.debug(f"Sending results to Soda Cloud {Emoticons.CLOUD}")
        return self._upload_scan_yaml_file(yaml_str=contract_yaml_source_str, soda_cloud_file_path=soda_cloud_file_path)

    def send_contract_result(self, contract_verification_result: ContractVerificationResult) -> Optional[dict]:
        """
        Returns A scanId string if a 200 OK was received, None otherwise
        """
        contract_verification_result = _build_contract_result_json_dict(
            contract_verification_result=contract_verification_result
        )
        contract_verification_result["type"] = "sodaCoreInsertScanResults"
        response: Response = self._execute_command(
            command_json_dict=contract_verification_result, request_log_name="send_contract_verification_results"
        )
        if response.status_code == 200:
            logger.info(f"{Emoticons.OK_HAND} Results sent to Soda Cloud")
            response_json: dict = response.json()
            if isinstance(response_json, dict):
                cloud_url: Optional[str] = response_json.get("cloudUrl")
                if isinstance(cloud_url, str):
                    logger.info(f"To view the dataset on Soda Cloud, see {cloud_url}")
                return response_json
        return None

    def send_contract_skeleton(self, contract_yaml_str: str, soda_cloud_file_path: str) -> None:
        file_id: Optional[str] = self._upload_scan_yaml_file(
            yaml_str=contract_yaml_str, soda_cloud_file_path=soda_cloud_file_path
        )
        if file_id:
            command_json_dict: dict = {
                "type": "sodaCoreUpsertDraftContract",
                "contract": {
                    "fileId": file_id,
                    "metadata": {"source": {"filePath": soda_cloud_file_path, "type": "cloud"}},
                },
            }
            response: Response = self._execute_command(
                command_json_dict=command_json_dict, request_log_name="create_draft_contract"
            )

            logger.debug(f"Response from Soda Cloud: {response.json()}")

    def _upload_scan_yaml_file(
        self,
        yaml_str: str,
        soda_cloud_file_path: str,
    ) -> Optional[str]:
        """
        Returns a Soda Cloud fileId or None if something is wrong.
        """
        try:
            with TemporaryFile() as temp_file:
                rows_json_bytes = bytearray(yaml_str, "utf-8")
                temp_file.write(rows_json_bytes)

                file_size_in_bytes = temp_file.tell()
                temp_file.seek(0)

                headers = {
                    "Authorization": self._get_token(),
                    "Content-Type": "application/yaml",
                    "Is-V3": "true",
                    "File-Path": soda_cloud_file_path,
                    "Soda-Library-Version": clean_soda_core_version(),
                }

                if file_size_in_bytes == 0:
                    # because of https://github.com/psf/requests/issues/4215 we can't send content size
                    # when the size is 0 since requests blocks then on I/O indefinitely
                    logger.warning("Empty file upload detected, not sending Content-Length header")
                else:
                    headers["Content-Length"] = str(file_size_in_bytes)

                upload_response = self._http_post(url=f"{self.api_url}/scan/upload", headers=headers, data=temp_file)
                upload_response_json = upload_response.json()

                if isinstance(upload_response_json, dict) and "fileId" in upload_response_json:
                    return upload_response_json.get("fileId")
                else:
                    logger.critical(f"No fileId received in response: {upload_response_json}")
                    return None
        except Exception as e:
            logger.critical(
                msg=f"Soda cloud error: Could not upload contract " f"to Soda Cloud: {e}",
                exc_info=True,
            )

    def test_connection(self) -> None:
        """
        Returns an error message or None if the connection test is successful
        """
        query: dict = {"type": "whoAmI"}
        self._execute_query(query_json_dict=query, request_log_name="who_am_i")

    def publish_contract(self, contract_yaml: Optional[ContractYaml]) -> ContractPublicationResult:
        if not contract_yaml:
            return ContractPublicationResult(contract=None)

        logger.info(
            f"Publishing {Emoticons.SCROLL} contract {contract_yaml.contract_yaml_source.file_path} "
            f"{Emoticons.FINGERS_CROSSED}"
        )
        contract_yaml_str_original = contract_yaml.contract_yaml_source.yaml_str_original
        contract_local_file_path = contract_yaml.contract_yaml_source.file_path

        dataset_identifier = DatasetIdentifier.parse(contract_yaml.dataset)

        can_publish_and_verify, reason = self.can_publish_and_verify_contract(
            dataset_identifier.data_source_name, dataset_identifier.prefixes, dataset_identifier.dataset_name
        )
        if not can_publish_and_verify or not contract_yaml_str_original:
            if reason is None:
                logger.error(f"Skipping contract publication because of an error (see logs)")
            else:
                logger.error(f"Skipping contract publication because of insufficient permissions: {reason}")
            return ContractPublicationResult(contract=None)

        soda_cloud_file_path: str = (
            contract_local_file_path if isinstance(contract_local_file_path, str) else "contract.yml"
        )
        file_id: Optional[str] = self._upload_scan_yaml_file(
            yaml_str=contract_yaml_str_original, soda_cloud_file_path=soda_cloud_file_path
        )
        if not file_id:
            logger.critical("Uploading the contract file failed")
            return ContractPublicationResult(contract=None)

        publish_contract_command: dict = {
            "type": "sodaCorePublishContract",
            "contract": {
                "fileId": file_id,
                "metadata": {"source": {"type": "local", "filePath": contract_local_file_path}},
            },
        }
        response: Response = self._execute_command(
            command_json_dict=publish_contract_command, request_log_name="publish_contract"
        )
        response_json = response.json()

        if response.status_code != 200:
            error_details = response_json.get("message", response.text)
            raise SodaCloudException(error_details)

        logger.info(f"{Emoticons.OK_HAND} Contract published on Soda Cloud")

        source_metadata = (
            response_json["metadata"]["source"]
            if "metadata" in response_json and "source" in response_json["metadata"]
            else {}
        )
        yaml_file_path = source_metadata["filePath"] if "filePath" in source_metadata else None

        return ContractPublicationResult(
            contract=Contract(
                data_source_name=dataset_identifier.data_source_name,
                dataset_prefix=dataset_identifier.prefixes,
                dataset_name=dataset_identifier.dataset_name,
                source=YamlFileContentInfo(
                    local_file_path=yaml_file_path,
                    source_content_str=None,
                    soda_cloud_file_id=response_json.get("fileId", None),
                ),
                soda_qualified_dataset_name=contract_yaml.dataset,
            ),
        )

    CONTRACT_PERMISSION_REASON_TEXTS = {
        "missingCanCreateDatasourceAndDataset": "The contract doesn't exist and the user can't create new contract "
        "since it would demand creation of dataset and datasource, but that permission is not available",
        "missingManageContracts": "The user can't release a new version of the contract or run a scan - "
        "they are not allowed to manage contracts",
    }

    def can_publish_and_verify_contract(
        self,
        data_source_name: str,
        dataset_prefix: list[str],
        dataset_name: str,
    ) -> tuple[bool, Optional[str]]:
        allowed, reason = self._get_contract_permissions(data_source_name, dataset_prefix, dataset_name)
        if reason and not reason in self.CONTRACT_PERMISSION_REASON_TEXTS.keys():
            logger.critical(f"Received unknown contract permission reason: {reason}")
        return allowed, self.CONTRACT_PERMISSION_REASON_TEXTS.get(reason, None)

    def _get_contract_permissions(
        self,
        data_source_name: str,
        dataset_prefix: list[str],
        dataset_name: str,
    ) -> tuple[bool, Optional[str]]:
        dataset_responsibilities_query: dict = {
            "type": "sodaCoreCanManageContracts",
            "dataset": {
                "datasource": data_source_name,
                "prefixes": dataset_prefix,
                "name": dataset_name,
            },
        }
        response: Response = self._execute_query(
            query_json_dict=dataset_responsibilities_query, request_log_name="can_manage_contracts"
        )
        if not response.status_code == 200:  # TODO: should this also not be a critical issue causing the scan to fail?
            return False, None
        response_json: dict = response.json()
        if not isinstance(response_json, dict):
            return False, None
        allowed: bool = response_json.get("allowed", False)
        reason: Optional[str] = response_json.get("reason", None)

        return allowed, reason

    def verify_contract_on_agent(
        self,
        contract_yaml: ContractYaml,
        variables: dict[str, str],
        blocking_timeout_in_minutes: int,
        publish_results: bool,
        verbose: bool,
    ) -> ContractVerificationResult:
        contract_yaml_str_original: str = contract_yaml.contract_yaml_source.yaml_str_original
        contract_local_file_path: Optional[str] = contract_yaml.contract_yaml_source.file_path or "REMOTE"  # TODO

        dataset_identifier = DatasetIdentifier.parse(contract_yaml.dataset)

        verification_result = ContractVerificationResult(
            contract=Contract(
                data_source_name=dataset_identifier.data_source_name,
                dataset_prefix=dataset_identifier.prefixes,
                dataset_name=dataset_identifier.dataset_name,
                soda_qualified_dataset_name=None,
                source=YamlFileContentInfo(
                    local_file_path=contract_local_file_path,
                    source_content_str=contract_yaml_str_original,
                    soda_cloud_file_id=None,
                ),
            ),
            data_source=None,
            data_timestamp=None,
            started_timestamp=None,
            ended_timestamp=None,
            measurements=[],
            check_results=[],
            sending_results_to_soda_cloud_failed=False,
            log_records=None,
            status=ContractVerificationStatus.UNKNOWN,
        )

        can_publish_and_verify, reason = self.can_publish_and_verify_contract(
            dataset_identifier.data_source_name, dataset_identifier.prefixes, dataset_identifier.dataset_name
        )
        if not can_publish_and_verify:
            if reason is None:
                logger.error(f"Skipping contract verification because of an error (see logs)")
                verification_result.sending_results_to_soda_cloud_failed = True
            else:
                logger.error(f"Skipping contract verification because of insufficient permissions: {reason}")
            return verification_result

        soda_cloud_file_path: str = (
            contract_local_file_path if isinstance(contract_local_file_path, str) else "contract.yml"
        )
        file_id: Optional[str] = self._upload_scan_yaml_file(
            yaml_str=contract_yaml_str_original, soda_cloud_file_path=soda_cloud_file_path
        )
        if not file_id:
            logger.critical(f"Contract wasn't uploaded so skipping " "sending the results to Soda Cloud")
            return []

        verify_contract_command: dict = {
            "type": "sodaCoreVerifyContract" if publish_results else "sodaCoreTestContract",
            "contract": {
                "fileId": file_id,
                "metadata": {"source": {"type": "local", "filePath": contract_local_file_path}},
            },
            "verbose": verbose,
            "variables": variables,
        }
        response: Response = self._execute_command(
            command_json_dict=verify_contract_command, request_log_name="verify_contract"
        )
        response_json: dict = response.json()
        scan_id: str = response_json.get("scanId")

        log_records: Optional[list[LogRecord]] = None
        if response.status_code != 200:
            logger.error("Remote contract verification failed.")
            verification_result.sending_results_to_soda_cloud_failed = True
            return verification_result

        if not scan_id:
            logger.warning("Did not receive a Scan ID from Soda Cloud")
            return verification_result

        scan_is_finished, contract_dataset_cloud_url, scan_status = self._poll_remote_scan_finished(
            scan_id=scan_id, blocking_timeout_in_minutes=blocking_timeout_in_minutes
        )

        verification_result.status = _map_remote_scan_status_to_contract_verification_status(scan_status)

        logger.debug(f"Asking Soda Cloud the logs of scan {scan_id}")
        logs_response: Response = self._get_scan_logs(scan_id=scan_id)
        logger.debug(f"Soda Cloud responded with {json.dumps(dict(logs_response.headers))}\n{logs_response.text}")

        # Start capturing logs here
        logs: Logs = Logs()

        response_json: dict = logs_response.json()
        soda_cloud_log_dicts: list[dict] = response_json.get("content")
        # TODO implement extra page loading if there are more pages of scan logs....
        # response body: {
        #   "content": [...],
        #   "totalElements": 0,
        #   "totalPages": 0,
        #   "number": 0,
        #   "size": 0,
        #   "last": true,
        #   "first": true
        # }
        if isinstance(soda_cloud_log_dicts, list):
            # For explanation, see TimestampToCreatedLoggingFilter
            logging_filter: TimestampToCreatedLoggingFilter = TimestampToCreatedLoggingFilter()
            logger.addFilter(logging_filter)
            try:
                for soda_cloud_log_dict in soda_cloud_log_dicts:
                    if isinstance(soda_cloud_log_dict, dict):
                        extra: dict = {}

                        level_cloud: str = soda_cloud_log_dict.get("level")
                        level_logrecord: int = logging.getLevelName(level_cloud.upper())

                        timestamp_cloud: str = soda_cloud_log_dict.get("timestamp")
                        timestamp_datetime: datetime = convert_str_to_datetime(timestamp_cloud)
                        if isinstance(timestamp_datetime, datetime):
                            timestamp_logrecord: float = timestamp_datetime.timestamp() + (
                                timestamp_datetime.microsecond / 1000000
                            )
                            # For explanation, see TimestampToCreatedLoggingFilter
                            extra[TimestampToCreatedLoggingFilter.TIMESTAMP] = timestamp_logrecord

                        location_dict: Optional[dict] = soda_cloud_log_dict.get(ExtraKeys.LOCATION)
                        if isinstance(location_dict, dict):
                            extra[ExtraKeys.LOCATION] = Location(
                                file_path=location_dict.get("filePath"),
                                line=location_dict.get("line"),
                                column=location_dict.get("col"),
                            )

                        doc: Optional[str] = soda_cloud_log_dict.get(ExtraKeys.DOC)
                        if doc:
                            extra[ExtraKeys.DOC] = doc

                        exception: Optional[str] = soda_cloud_log_dict.get(ExtraKeys.EXCEPTION)
                        if doc:
                            extra[ExtraKeys.EXCEPTION] = exception

                        logger.log(level=level_logrecord, msg=soda_cloud_log_dict.get("message"), extra=extra)
                    else:
                        logger.debug(
                            f"Expected dict for logs list element, but was {type(soda_cloud_log_dict).__name__}"
                        )
            finally:
                logger.removeFilter(logging_filter)

        elif soda_cloud_log_dicts is None:
            logger.debug(f"No logs in Soda Cloud response")
        else:
            logger.debug(f"Expected dict for logs, but was {type(soda_cloud_log_dicts).__name__}")

        if not scan_is_finished:
            logger.error(f"Max retries exceeded. " f"Contract verification did not finish yet.")
            verification_result.sending_results_to_soda_cloud_failed = True

        if contract_dataset_cloud_url:
            logger.info(f"See contract dataset on Soda Cloud: {contract_dataset_cloud_url}")

        logs.remove_from_root_logger()
        log_records = logs.records

        verification_result.log_records = logs.records

        return verification_result

    def fetch_contract_for_dataset(self, dataset_identifier: str) -> str:
        """Fetch the contract contents for the given dataset identifier.

        Returns:
            The contract content as a string, or None if:
            - the data source or dataset does not exist
            - no contract is linked to the dataset
            - an unexpected response is received from the backend
        """

        logger.info(f"{Emoticons.SCROLL} Fetching contract from Soda Cloud for dataset '{dataset_identifier}'")
        parsed_identifier = DatasetIdentifier.parse(dataset_identifier)
        request = {
            "type": "sodaCoreGetContract",
            "dataset": {
                "datasource": parsed_identifier.data_source_name,
                "prefixes": parsed_identifier.prefixes,
                "name": parsed_identifier.dataset_name,
            },
        }
        response = self._execute_query(request, request_log_name="get_contract")
        response_dict = response.json()

        if response.status_code == 400:
            error_code = response_dict.get("code")

            if error_code == "contract_not_found":
                raise ContractNotFoundException(parsed_identifier)
            elif error_code == "datasource_not_found":
                raise DataSourceNotFoundException(parsed_identifier)
            elif error_code == "dataset_not_found":
                raise DatasetNotFoundException(parsed_identifier)

        if response.status_code != 200:
            raise SodaCloudException(f"Failed to retrieve contract contents for dataset '{str(dataset_identifier)}'")

        return response_dict.get("contents")

    def fetch_data_source_configuration_for_dataset(self, dataset_identifier: str) -> str:
        """Fetches the data source configuration for the source associated with the given dataset identifier.

        Returns:
            The data source configuration content as a string, or None if:
            - the data source or dataset does not exist
            - an unexpected response is received from the backend
        """

        logger.info(
            f"{Emoticons.CLOUD} Fetching data source configuration from Soda Cloud for dataset '{dataset_identifier}'"
        )
        parsed_identifier = DatasetIdentifier.parse(dataset_identifier)
        request = {
            "type": "sodaCoreGetDatasourceConfigurationFile",
            "dataset": {
                "datasource": parsed_identifier.data_source_name,
                "prefixes": parsed_identifier.prefixes,
                "name": parsed_identifier.dataset_name,
            },
        }
        response = self._execute_query(request, request_log_name="get_contract_data_source_configuration")
        response_dict = response.json()

        if response.status_code == 400:
            if response_dict["code"] == "datasource_not_found":
                raise DataSourceNotFoundException(parsed_identifier)

        if response.status_code != 200:
            raise SodaCloudException(
                f"Failed to retrieve data source configuration for dataset '{dataset_identifier}': {response_dict['message']}"
            )

        return response_dict.get("contents")

    def _poll_remote_scan_finished(
        self, scan_id: str, blocking_timeout_in_minutes: int
    ) -> tuple[bool, Optional[str], Optional[RemoteScanStatus]]:
        """
        Returns a tuple of 2 values:
        * A boolean indicating if the scan finished (true means scan finished. false means there was a timeout or retry exceeded)
        * The Soda Cloud URL that navigates to the scan.  If it was obtained from Soda Cloud.
        """

        blocking_timeout = datetime.now() + timedelta(minutes=blocking_timeout_in_minutes)
        attempt = 0
        while datetime.now() < blocking_timeout:
            attempt += 1
            max_wait: timedelta = blocking_timeout - datetime.now()
            logger.debug(
                f"Asking Soda Cloud if scan {scan_id} is already completed. Attempt {attempt}. Max wait: {max_wait}"
            )
            response = self._get_scan_status(scan_id)
            logger.debug(f"Soda Cloud responded with {json.dumps(dict(response.headers))}\n{response.text}")
            if not response:
                logger.error(f"Failed to poll remote scan status. " f"Response: {response}")
                continue

            response_body_dict: Optional[dict] = response.json() if response else None
            contract_dataset_cloud_url: Optional[str] = (
                response_body_dict.get("contractDatasetCloudUrl") if response_body_dict else None
            )

            if "state" not in response_body_dict:
                continue
            scan_state = RemoteScanStatus.from_value(response_body_dict["state"])

            logger.info(f"Scan {scan_id} has state '{scan_state.value_}'")

            if scan_state.is_final_state:
                return True, contract_dataset_cloud_url, scan_state

            time_to_wait_in_seconds: float = 5
            next_poll_time_str = response.headers.get("X-Soda-Next-Poll-Time")
            if next_poll_time_str:
                logger.debug(
                    f"Soda Cloud suggested to ask scan {scan_id} status again at '{next_poll_time_str}' "
                    f"via header X-Soda-Next-Poll-Time"
                )
                next_poll_time: datetime = convert_str_to_datetime(next_poll_time_str)
                if isinstance(next_poll_time, datetime):
                    now = datetime.now(timezone.utc)
                    time_to_wait = next_poll_time - now
                    time_to_wait_in_seconds = time_to_wait.total_seconds()
                else:
                    time_to_wait_in_seconds = 60
            if time_to_wait_in_seconds > 0:
                logger.debug(
                    f"Sleeping {time_to_wait_in_seconds} seconds before asking "
                    f"Soda Cloud scan {scan_id} status again in ."
                )
                sleep(time_to_wait_in_seconds)

        return False, None, None

    def _get_scan_status(self, scan_id: str) -> Response:
        return self._execute_rest_get(
            relative_url_path=f"scans/{scan_id}",
            request_log_name="get_scan_status",
        )

    def _get_scan_logs(self, scan_id: str) -> Response:
        return self._execute_rest_get(
            relative_url_path=f"scans/{scan_id}/logs",
            request_log_name="get_scan_logs",
        )

    def _execute_query(self, query_json_dict: dict, request_log_name: str) -> Response:
        return self._execute_cqrs_request(
            request_type="query", request_log_name=request_log_name, request_body=query_json_dict, is_retry=True
        )

    def _execute_command(self, command_json_dict: dict, request_log_name: str) -> Response:
        return self._execute_cqrs_request(
            request_type="command", request_log_name=request_log_name, request_body=command_json_dict, is_retry=True
        )

    def _execute_cqrs_request(
        self, request_type: str, request_log_name: str, request_body: dict, is_retry: bool
    ) -> Response:
        try:
            request_body["token"] = self._get_token()
            log_body_text: str = json.dumps(to_jsonnable(request_body), indent=2)
            logger.debug(f"Sending {request_type} {request_log_name} to Soda Cloud with body: {log_body_text}")
            response: Response = self._http_post(
                url=f"{self.api_url}/{request_type}",
                headers=self.headers,
                json=request_body,
                request_log_name=request_log_name,
            )

            trace_id: str = response.headers.get("X-Soda-Trace-Id")
            # TODO let m1no check if this is still needed...
            # if request_name:
            #     trace_id = response.headers.get("X-Soda-Trace-Id")
            #
            #     if trace_id:
            #         self.soda_cloud_trace_ids[request_name] = trace_id

            if response.status_code == 401 and is_retry:
                logger.debug(
                    f"Soda Cloud authentication failed for {request_type} {request_log_name}. "
                    f"Probably token expired. Re-authenticating... | X-Soda-Trace-Id:{trace_id}"
                )
                self.token = None
                response = self._execute_cqrs_request(
                    request_type=request_type,
                    request_log_name=request_log_name,
                    request_body=request_body,
                    is_retry=False,
                )
            elif response.status_code != 200:
                verbose_message: str = (
                    "" if logger.isEnabledFor(logging.DEBUG) else "Enable verbose mode to see more details."
                )
                logger.error(f"Soda Cloud error for {request_type} `{request_log_name}`. {verbose_message}")
                logger.debug(
                    f"Status_code:{response.status_code} | "
                    f"X-Soda-Trace-Id:{trace_id} | response_text:{response.text}"
                )
            else:
                logger.debug(
                    f"{Emoticons.OK_HAND} Soda Cloud {request_type} {request_log_name} OK | X-Soda-Trace-Id:{trace_id}"
                )

            return response
        except SodaCloudAuthenticationFailedException:
            raise
        except Exception as e:
            logger.critical(
                msg=f"Error while executing Soda Cloud {request_type} {request_log_name}",
                exc_info=True,
            )

    def _http_post(self, request_log_name: str = None, **kwargs) -> Response:
        return requests.post(**kwargs)

    def _execute_rest_get(self, relative_url_path: str, request_log_name: str, is_retry: bool = True) -> Response:
        credentials_plain = f"{self.api_key_id}:{self.api_key_secret}"
        credentials_encoded = base64.b64encode(credentials_plain.encode()).decode()

        headers = {
            "Authorization": f"Basic {credentials_encoded}",
            "Accept": "application/json",
        }

        url: str = f"{self.api_url}/v1/{relative_url_path}"
        logger.debug(f"Sending GET {url} request to Soda Cloud")
        response: Response = self._http_get(url=url, headers=headers)

        trace_id: str = response.headers.get("X-Soda-Trace-Id")

        if response.status_code == 401 and is_retry:
            logger.debug(
                f"Soda Cloud authentication failed. Probably token expired. Re-authenticating... | "
                f"X-Soda-Trace-Id:{trace_id}"
            )
            self.token = None
            response = self._execute_rest_get(
                relative_url_path=relative_url_path, request_log_name=request_log_name, is_retry=False
            )
        elif response.status_code != 200:
            logger.debug(
                f"Soda Cloud error for {request_log_name} | status_code:{response.status_code} | "
                f"X-Soda-Trace-Id:{trace_id} | response_text:{response.text}"
            )
        else:
            logger.debug(f"{Emoticons.OK_HAND} Soda Cloud {request_log_name} OK | X-Soda-Trace-Id:{trace_id}")

        return response

    def _http_get(self, **kwargs) -> Response:
        return requests.get(**kwargs)

    def _get_token(self) -> str:
        if not self.token:
            login_command = {"type": "login"}
            if self.api_key_id and self.api_key_secret:
                login_command["apiKeyId"] = self.api_key_id
                login_command["apiKeySecret"] = self.api_key_secret
            else:
                raise RuntimeError("No API KEY and/or SECRET provided ")

            login_response = self._http_post(
                url=f"{self.api_url}/command", headers=self.headers, json=login_command, request_log_name="get_token"
            )
            if login_response.status_code != 200:
                raise SodaCloudAuthenticationFailedException(
                    "Soda Cloud authentication failed. The provided API keys are unknown or invalid. "
                    "Please verify your credentials."
                )
            login_response_json = login_response.json()

            self.token = login_response_json.get("token")
            assert self.token, "No token in login response?!"
        return self.token

    def send_failed_rows_diagnostics(self, scan_id: str, failed_rows_diagnostics: list[FailedRowsDiagnostic]):
        print(f"TODO sending failed rows diagnostics for scan {scan_id} to Soda Cloud: {failed_rows_diagnostics}")


def to_jsonnable(o: Any, remove_null_values_in_dicts: bool = True) -> object:
    if o is None or isinstance(o, str) or isinstance(o, int) or isinstance(o, float) or isinstance(o, bool):
        return o
    if isinstance(o, dict):
        for key in list(o.keys()):
            value = o[key]
            if value is not None:
                update = False
                if not isinstance(key, str):
                    del o[key]
                    key = str(key)
                    update = True

                jsonnable_value = to_jsonnable(value)
                if value is not jsonnable_value:
                    value = jsonnable_value
                    update = True
                if update:
                    o[key] = value
            elif remove_null_values_in_dicts:
                del o[key]
        return o
    if isinstance(o, tuple):
        return to_jsonnable(list(o))
    if isinstance(o, list):
        for i in range(len(o)):
            element = o[i]
            jsonnable_element = to_jsonnable(element)
            if element is not jsonnable_element:
                o[i] = jsonnable_element
        return o
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, datetime):
        return convert_datetime_to_str(o)
    if isinstance(o, date):
        return o.strftime("%Y-%m-%d")
    if isinstance(o, time):
        return o.strftime("%H:%M:%S")
    if isinstance(o, timedelta):
        return str(o)
    if isinstance(o, Enum):
        return o.value
    if isinstance(o, Exception):
        return str(o)
    raise RuntimeError(f"Do not know how to jsonize {o} ({type(o)})")


def _build_check_results_cloud_json_dicts(
    contract_verification_result: ContractVerificationResult,
) -> Optional[list[dict]]:
    check_results: Optional[list[CheckResult]] = contract_verification_result.check_results
    contract: Contract = contract_verification_result.contract
    if not check_results:
        return None
    return [
        _build_check_result_cloud_dict(contract=contract, check_result=check_result) for check_result in check_results
    ]


def _build_scan_definition_name(contract_verification_result: ContractVerificationResult) -> str:
    scan_definition_name: str = os.environ.get("SODA_SCAN_DEFINITION")
    if scan_definition_name:
        logger.debug(f"Using SODA_SCAN_DEFINITION from environment variable: {scan_definition_name}")
        return scan_definition_name
    else:
        return contract_verification_result.contract.soda_qualified_dataset_name


def _build_contract_result_json_dict(contract_verification_result: ContractVerificationResult) -> dict:
    return to_jsonnable(  # type: ignore
        {
            "scanId": os.environ.get("SODA_SCAN_ID", None),
            # The scan definition name is still required on result ingestion to link to the contract
            # and determine if we're dealing with a default or test contract.
            "definitionName": _build_scan_definition_name(contract_verification_result),
            "defaultDataSource": contract_verification_result.data_source.name,
            "defaultDataSourceProperties": {"type": contract_verification_result.data_source.type},
            # dataTimestamp can be changed by user, this is shown in Cloud as time of a scan.
            # It's the timestamp used to identify the time partition, which is the slice of data that is verified.
            "dataTimestamp": contract_verification_result.data_timestamp,
            # scanStartTimestamp is the actual time when the scan started.
            "scanStartTimestamp": contract_verification_result.started_timestamp,
            # scanEndTimestamp is the actual time when scan ended.
            "scanEndTimestamp": contract_verification_result.ended_timestamp,
            "hasErrors": contract_verification_result.has_errors(),
            "hasWarnings": False,
            "hasFailures": contract_verification_result.is_failed(),
            "checks": _build_check_results_cloud_json_dicts(contract_verification_result),
            "logs": _build_log_cloud_json_dicts(contract_verification_result.log_records),
            "sourceOwner": "soda-core",
            "contract": _build_contract_cloud_json_dict(contract_verification_result.contract),
        }
    )


def _build_contract_cloud_json_dict(contract: Contract):
    return {
        "fileId": contract.source.soda_cloud_file_id,
        "metadata": {
            "source": {
                "type": "local",
                # TODO: make contract metadata verification optional on BE?
                "filePath": contract.source.local_file_path or "REMOTE",
            }
        },
    }


def _build_check_result_cloud_dict(contract: Contract, check_result: CheckResult) -> dict:
    return {
        "identities": {"vc1": check_result.check.identity},
        "checkPath": _build_check_path(check_result),
        "name": check_result.check.name,
        "type": "generic",
        "checkType": check_result.check.type,
        "definition": check_result.check.definition,
        **_build_check_attributes(check_result.check.attributes).model_dump(by_alias=True),
        "location": _build_check_location(check_result),
        "dataSource": contract.data_source_name,
        "table": contract.dataset_name,
        "datasetPrefix": contract.dataset_prefix,
        "column": check_result.check.column_name,
        "outcome": _build_check_outcome_for_soda_cloud(check_result.outcome),
        "source": "soda-contract",
        "diagnostics": _build_diagnostics_json_dict(check_result),
    }


def _build_check_location(check_result: CheckResult) -> dict:
    return {
        "filePath": _build_file_path(check_result.check.location),
        "line": check_result.check.contract_file_line,
        "col": check_result.check.contract_file_column,
    }


def _build_file_path(location: Location) -> str:
    return location.file_path if isinstance(location.file_path, str) else "yamlstr.yml"


def _build_check_outcome_for_soda_cloud(outcome: CheckOutcome) -> str:
    if outcome == CheckOutcome.PASSED:
        return "pass"
    elif outcome == CheckOutcome.FAILED:
        return "fail"
    return "unevaluated"


def _build_diagnostics_json_dict(check_result: CheckResult) -> Optional[dict]:
    return {
        #  TODO: this default 0 value is here only because check.diagnostics.value is a required non-nullable field in the api.
        "value": check_result.get_threshold_value() or 0,
        "fail": _build_fail_threshold(check_result),
        "v4": _build_v4_diagnostics_check_type_json_dict(check_result),
    }


def _build_v4_diagnostics_check_type_json_dict(check_result: CheckResult) -> Optional[dict]:
    from soda_core.contracts.impl.check_types.freshness_check import (
        FreshnessCheckResult,
    )
    from soda_core.contracts.impl.check_types.schema_check import SchemaCheckResult

    if check_result.check.type == "missing":
        return {
            "type": check_result.check.type,
            "failedRowsCount": check_result.diagnostic_metric_values.get("missing_count"),
            "failedRowsPercent": check_result.diagnostic_metric_values.get("missing_percent"),
            "datasetRowsTested": check_result.diagnostic_metric_values.get("dataset_rows_tested"),
            "checkRowsTested": check_result.diagnostic_metric_values.get("row_count"),
        }
    elif check_result.check.type == "invalid":
        return {
            "type": check_result.check.type,
            "failedRowsCount": check_result.diagnostic_metric_values.get("invalid_count"),
            "failedRowsPercent": check_result.diagnostic_metric_values.get("invalid_percent"),
            "datasetRowsTested": check_result.diagnostic_metric_values.get("dataset_rows_tested"),
            "checkRowsTested": check_result.diagnostic_metric_values.get("row_count"),
        }
    elif check_result.check.type == "duplicate":
        return {
            "type": check_result.check.type,
            "failedRowsCount": check_result.diagnostic_metric_values.get("duplicate_count"),
            "failedRowsPercent": check_result.diagnostic_metric_values.get("duplicate_percent"),
            "datasetRowsTested": check_result.diagnostic_metric_values.get("dataset_rows_tested"),
            "checkRowsTested": check_result.diagnostic_metric_values.get("row_count"),
        }
    elif check_result.check.type == "failed_rows":
        return {
            "type": check_result.check.type,
            "failedRowsCount": check_result.diagnostic_metric_values.get("failed_rows_count"),
            "failedRowsPercent": check_result.diagnostic_metric_values.get("failed_rows_percent"),
            "datasetRowsTested": check_result.diagnostic_metric_values.get("dataset_rows_tested"),
            # There is no check filter allowed for failed rows so no check rows tested is needed
        }
    elif check_result.check.type == "aggregate":
        return {
            "type": check_result.check.type,
            "datasetRowsTested": check_result.diagnostic_metric_values.get("dataset_rows_tested"),
            # We skip the checkRowsTested because it causes extra compute.
            # To be re-evaluated when we have users asking for it.
        }
    elif check_result.check.type == "metric":
        return {
            "type": check_result.check.type,
            "datasetRowsTested": check_result.diagnostic_metric_values.get("dataset_rows_tested"),
        }
    elif check_result.check.type == "row_count":
        return {
            "type": check_result.check.type,
            "checkRowsTested": check_result.diagnostic_metric_values.get("row_count"),
            "datasetRowsTested": check_result.diagnostic_metric_values.get("dataset_rows_tested"),
        }
    elif isinstance(check_result, SchemaCheckResult):
        return {
            "type": check_result.check.type,
            "expected": [_build_schema_column(expected) for expected in check_result.expected_columns],
            "actual": [_build_schema_column(actual) for actual in check_result.actual_columns],
            # To be added later if we need it
            # "actualColumnsNotExpected": [...],
            # "expectedColumnsNotActual": [...],
            # "columnDataTypeMismatches": [...],
        }
    elif isinstance(check_result, FreshnessCheckResult):
        return {
            "type": "freshness",
            # Check the console logs!
            "actualTimestamp": convert_datetime_to_str(check_result.max_timestamp),
            "actualTimestampUtc": convert_datetime_to_str(check_result.max_timestamp_utc),
            "expectedTimestamp": convert_datetime_to_str(check_result.data_timestamp),
            "expectedTimestampUtc": convert_datetime_to_str(check_result.data_timestamp_utc),
            "datasetRowsTested": check_result.diagnostic_metric_values.get("dataset_rows_tested"),
            # We skip the checkRowsTested because it causes extra compute.
            # To be re-evaluated when we have users asking for it.
        }
    raise SodaException(f"Invalid check type: {type(check_result.check).__name__}")


def _build_schema_column(column_metadata: ColumnMetadata) -> Optional[dict]:
    return {
        "name": column_metadata.column_name,
        # The type includes the max char length if specified in the metadata eg VARCHAR(255)
        "type": column_metadata.get_data_type_ddl(),
    }


def _build_fail_threshold(check_result: CheckResult) -> Optional[dict]:
    threshold: Threshold = check_result.check.threshold
    if threshold:
        return {
            "greaterThan": threshold.must_be_less_than_or_equal,
            "greaterThanOrEqual": threshold.must_be_less_than,
            "lessThan": threshold.must_be_greater_than_or_equal,
            "lessThanOrEqual": threshold.must_be_greater_than,
        }
    return None


def _map_remote_scan_status_to_contract_verification_status(
    scan_status: RemoteScanStatus,
) -> ContractVerificationStatus:
    if scan_status in (RemoteScanStatus.COMPLETED, RemoteScanStatus.COMPLETED_WITH_WARNINGS):
        return ContractVerificationStatus.PASSED
    elif scan_status in (RemoteScanStatus.COMPLETED_WITH_FAILURES, RemoteScanStatus.FAILED):
        return ContractVerificationStatus.FAILED
    elif scan_status in RemoteScanStatus.COMPLETED_WITH_ERRORS:
        return ContractVerificationStatus.ERROR
    else:
        return ContractVerificationStatus.UNKNOWN


# def _build_diagnostics_column_data_type_mismatches(
#     column_data_type_mismatches: list["ColumnDataTypeMismatch"],
# ) -> list[SodaCloudSchemaDataTypeMismatch]:
#     return [
#         SodaCloudSchemaDataTypeMismatch(
#             column=column_data_type_mismatch.column,
#             expectedDataType=column_data_type_mismatch.expected_data_type,
#             actualDataType=column_data_type_mismatch.actual_data_type,
#             expectedCharacterMaximumLength=column_data_type_mismatch.expected_character_maximum_length,
#             actualCharacterMaximumLength=column_data_type_mismatch.actual_character_maximum_length,
#         )
#         for column_data_type_mismatch in column_data_type_mismatches
#     ]


def _build_check_path(check_result: CheckResult) -> str:
    check: Check = check_result.check
    parts: list[str] = []
    if check.column_name:
        parts.append("columns")
        parts.append(check.column_name)
    parts.append("checks")
    parts.append(check_result.check.type)
    if check.qualifier:
        parts.append(check.qualifier)
    return ".".join(parts)


def _build_log_cloud_json_dicts(log_records: Optional[list[LogRecord]]) -> Optional[list[dict]]:
    if not log_records:
        return None
    return [_build_log_cloud_json_dict(log_record, index) for index, log_record in enumerate(log_records)]


def _build_log_cloud_json_dict(log_record: LogRecord, index: int) -> dict:
    return {
        "level": log_record.levelname.lower(),
        "message": log_record.msg,
        "timestamp": datetime.fromtimestamp(log_record.created),
        "index": index,
        "doc": log_record.doc if hasattr(log_record, "doc") else None,
        "exception": log_record.exception if hasattr(log_record, "exception") else None,
        "location": (
            log_record.location.get_dict()
            if hasattr(log_record, ExtraKeys.LOCATION) and isinstance(log_record.location, Location)
            else None
        ),
    }


def _exception_to_cloud_log_dict(exception: Exception) -> dict:
    return {
        "level": "error",
        "message": f"An exception occurred: {str(exception)}",
        "timestamp": datetime.now(timezone.utc),
        "index": 0,
        "exception": str(exception),
        "location": None,
    }


def _append_exception_to_cloud_log_dicts(cloud_log_dicts: list[dict], exception: Exception) -> list[dict]:
    exc_cloud_log_dict = _exception_to_cloud_log_dict(exception)
    exc_cloud_log_dict["index"] = len(cloud_log_dicts)
    cloud_log_dicts.append(exc_cloud_log_dict)
    return cloud_log_dicts


def _build_check_attributes(data: dict[str, Any]) -> CheckAttributes:
    return CheckAttributes(check_attributes=[CheckAttribute.from_raw(k, v) for k, v in data.items()])
