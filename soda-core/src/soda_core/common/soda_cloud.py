from __future__ import annotations

import base64
import json
import logging
import os
import re
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from logging import LogRecord
from time import sleep
from typing import Any, Dict, Optional

import requests
from pydantic import ValidationError
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
    FailedContractSkeletonGenerationException,
    InvalidSodaCloudConfigurationException,
    SodaCloudAuthenticationFailedException,
    SodaCloudException,
)
from soda_core.common.logging_constants import Emoticons, ExtraKeys, soda_logger
from soda_core.common.logs import Location, Logs
from soda_core.common.soda_cloud_dto import (
    CheckAttribute,
    CheckAttributes,
    DatasetConfigurationDTO,
    DatasetConfigurationsDTO,
    RequestDatasetsConfigurationDatasetDTO,
    RequestDatasetsConfigurationDTO,
)
from soda_core.common.utils import to_camel_case
from soda_core.common.version import SODA_CORE_VERSION
from soda_core.common.yaml import SodaCloudYamlSource, YamlObject
from soda_core.contracts.contract_publication import ContractPublicationResult
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    Contract,
    ContractVerificationResult,
    ContractVerificationStatus,
    PostProcessingStageState,
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


class MigrationStatus(Enum):
    CREATED = "created"
    GENERATING_CONTRACTS = "generatingContracts"
    COMPLETED_CONTRACT_GENERATION = "completedContractGeneration"
    SUBMITTED_MIGRATION = "submittedMigration"
    MIGRATING = "migrating"
    COMPLETED_MIGRATION = "completedMigration"
    GENERATION_FAILED = "generationFailed"
    MIGRATION_FAILED = "migrationFailed"
    CANCELED = "canceled"

    @classmethod
    def from_value(cls, value: str) -> MigrationStatus:
        for status in cls:
            if status.value == value:
                return status
        raise ValueError(f"Unknown MigrationStatus value: {value}")

    @property
    def is_generate_final_state(self) -> bool:
        return self in {
            MigrationStatus.COMPLETED_CONTRACT_GENERATION,
            MigrationStatus.CANCELED,
            MigrationStatus.GENERATION_FAILED,
        }

    @property
    def is_generate_successful_state(self) -> bool:
        return self in {MigrationStatus.COMPLETED_CONTRACT_GENERATION}

    @property
    def is_publish_final_state(self) -> bool:
        return self in {MigrationStatus.COMPLETED_MIGRATION, MigrationStatus.CANCELED, MigrationStatus.MIGRATION_FAILED}

    @property
    def is_publish_successful_state(self) -> bool:
        return self in {MigrationStatus.COMPLETED_MIGRATION}


class DatasetMigrationStatus(Enum):
    CREATED = "created"
    GENERATING_CONTRACT = "generatingContract"
    COMPLETED_CONTRACT_GENERATION = "completedContractGeneration"
    SUBMITTED_MIGRATION = "submittedMigration"
    MIGRATING = "migrating"
    GENERATION_FAILED = "generationFailed"
    MIGRATION_FAILED = "migrationFailed"
    COMPLETED_MIGRATION = "completedMigration"
    CANCELED = "canceled"
    ALREADY_MIGRATING = "alreadyMigrating"
    ALREADY_MIGRATED = "alreadyMigrated"

    @property
    def is_generate_final_state(self) -> bool:
        return self in {
            DatasetMigrationStatus.COMPLETED_CONTRACT_GENERATION,
            DatasetMigrationStatus.CANCELED,
            DatasetMigrationStatus.GENERATION_FAILED,
        }

    @property
    def is_generate_successful_state(self) -> bool:
        return self in {DatasetMigrationStatus.COMPLETED_CONTRACT_GENERATION}

    @property
    def is_publish_final_state(self) -> bool:
        return self in {
            DatasetMigrationStatus.COMPLETED_MIGRATION,
            MigrationStatus.CANCELED,
            MigrationStatus.MIGRATION_FAILED,
        }

    @property
    def is_publish_successful_state(self) -> bool:
        return self in {
            DatasetMigrationStatus.COMPLETED_MIGRATION,
        }

    @property
    def is_publish_warning_state(self) -> bool:
        return self in {
            DatasetMigrationStatus.ALREADY_MIGRATED,
            DatasetMigrationStatus.ALREADY_MIGRATING,
        }


class ContractSkeletonGenerationState(Enum):
    PENDING = "pending"
    FAILED = "failed"
    COMPLETED = "completed"

    @classmethod
    def from_value(cls, value: str) -> ContractSkeletonGenerationState:
        for status in cls:
            if status.value == value:
                return status
        raise ValueError(f"Unknown ContractSkeletonGenerationState value: {value}")


class ContractType(Enum):
    DEFAULT = "default"
    TEST = "test"
    DRAFT = "draft"


class VerificationIngestionMode(Enum):
    FULL = "full"
    PARTIAL = "partial"


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

        soda_cloud_yaml_object: Optional[YamlObject] = soda_cloud_yaml_root_object.read_object("soda_cloud")

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
            [build_log_cloud_json_dict(log_record, index) for index, log_record in enumerate(logs)] if logs else []
        )

        if exc:
            cloud_log_dicts = _append_exception_to_cloud_log_dicts(cloud_log_dicts, exc)

        self._execute_command(
            command_json_dict={"type": "sodaCoreMarkScanFailed", "scanId": scan_id, "logs": cloud_log_dicts},
            request_log_name="mark_scan_as_failed",
        )

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

    def trigger_contract_skeleton_generation(self, dataset_identifier: DatasetIdentifier) -> None:
        command_json_dict: dict = {
            "type": "sodaCoreGenerateContractSkeleton",
            "datasetIdentifier": dataset_identifier.to_string(),
        }
        response: Response = self._execute_command(
            command_json_dict=command_json_dict, request_log_name="generate_contract_skeleton"
        )
        response_json = response.json()

        if response.status_code != 200:
            error_details = response_json.get("message", response.text)
            raise SodaCloudException(error_details)

        logger.info(f"{Emoticons.OK_HAND} Contract skeleton generation triggered on Soda Cloud")

    def send_contract_skeleton(self, contract_yaml_str: str, soda_cloud_file_path: str) -> None:
        file_id: Optional[str] = self._upload_contract_yaml_file(contract_yaml_str)
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

    def _upload_contract_yaml_file(self, contract_yaml: str) -> Optional[str]:
        """
        Returns a Soda Cloud fileId or None if something is wrong.
        """
        try:
            upload_contract_command: dict = {
                "type": "sodaCoreUploadContractFile",
                "contents": contract_yaml,
            }
            response: Response = self._execute_command(
                command_json_dict=upload_contract_command, request_log_name="upload_contract_file"
            )
            response_json = response.json()
            if isinstance(response_json, dict) and "fileId" in response_json:
                return response_json.get("fileId")
            else:
                logger.critical(f"No fileId received in response: {response_json}")
                return None
        except Exception as e:
            logger.critical(
                msg=f"Soda cloud error: Could not upload contract file to Soda Cloud: {e}",
                exc_info=True,
            )
            return None

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

        file_id: Optional[str] = self._upload_contract_yaml_file(contract_yaml_str_original)
        if not file_id:
            logger.critical("Uploading the contract file failed")
            return ContractPublicationResult(contract=None)

        publish_contract_command: dict = {
            "type": "sodaCorePublishContract",
            "contract": {
                "fileId": file_id,
                "metadata": {
                    "source": {
                        "type": "local",
                        "filePath": contract_local_file_path,
                    },
                },
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
                dataset_id=response_json.get("publishedContract", {}).get("datasetId", None),
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
                logger.error("Skipping contract verification because of an error (see logs)")
                verification_result.sending_results_to_soda_cloud_failed = True
            else:
                logger.error(f"Skipping contract verification because of insufficient permissions: {reason}")
            return verification_result

        file_id: Optional[str] = self._upload_contract_yaml_file(contract_yaml_str_original)
        if not file_id:
            logger.critical("Contract wasn't uploaded so skipping sending the results to Soda Cloud")
            return []

        verify_contract_command: dict = {
            "type": "sodaCoreVerifyContract" if publish_results else "sodaCoreTestContract",
            "contract": {
                "fileId": file_id,
                "metadata": {
                    "source": {
                        "type": "local",
                        "filePath": contract_local_file_path,
                    },
                },
            },
            "verbose": verbose,
            "variables": variables,
        }
        response: Response = self._execute_command(
            command_json_dict=verify_contract_command, request_log_name="verify_contract"
        )
        response_json: dict = response.json()
        scan_id: str = response_json.get("scanId")

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
                        if exception:
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
        verification_result.log_records = logs.get_log_records()

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

    def fetch_checks_for_dataset_id(self, dataset_id: str) -> list[dict]:
        response = self._execute_query(
            query_json_dict={
                "type": "sodaCoreV3MigrationCheckInformation",
                "datasetId": dataset_id,
            },
            request_log_name="get_migration_checks_for_dataset_id",
        )

        if response and response.ok and response.content:
            json_content = json.loads(response.content)
            return json_content.get("checks", [])
        return []

    def fetch_data_source_configuration_for_dataset(self, dataset_identifier: str) -> str:
        """Fetches the data source configuration for the source associated with the given dataset identifier.

        Returns:
            The data source configuration content as a string, or None if:
            - the data source or dataset does not exist
            - an unexpected response is received from the backend
        """

        logger.info(f"Fetching data source configuration from Soda Cloud for dataset '{dataset_identifier}'")
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

    def fetch_dataset_configuration(self, dataset_identifier: DatasetIdentifier) -> Optional[DatasetConfigurationDTO]:
        """Fetches the dataset configuration.

        Returns:
            DatasetConfigurationDTO or None
        """
        configs: DatasetConfigurationsDTO = self.fetch_datasets_configurations([dataset_identifier])
        if configs and configs.dataset_configurations and len(configs.dataset_configurations) > 0:
            assumed_configuration = configs.dataset_configurations[0]
            if assumed_configuration.dataset_qualified_name != dataset_identifier.to_string():
                raise SodaCloudException(
                    f"Expected to receive configuration for dataset '{dataset_identifier}' but received configuration for dataset '{assumed_configuration.dataset_qualified_name}'"
                )
            return assumed_configuration
        return None

    def fetch_datasets_configurations(self, dataset_identifiers: list[DatasetIdentifier]) -> DatasetConfigurationsDTO:
        """Fetches the datasets configurations.

        Returns:
            DatasetConfigurationsDTO
        """
        logger.info(f"Fetching datasets configurations from Soda Cloud for datasets '{dataset_identifiers}'")
        datasets_request_list = []
        for dataset_identifier in dataset_identifiers:
            datasets_request_list.append(
                RequestDatasetsConfigurationDatasetDTO(
                    dataset_qualified_name=dataset_identifier.to_string(),
                    table=dataset_identifier.dataset_name,
                    data_source=dataset_identifier.data_source_name,
                )
            )
        request_dto: RequestDatasetsConfigurationDTO = RequestDatasetsConfigurationDTO(datasets=datasets_request_list)

        response = self._execute_query(
            request_dto.model_dump(by_alias=True), request_log_name="get_datasets_configurations"
        )
        response_dict = response.json()

        if not response.ok:
            raise SodaCloudException(
                f"Failed to retrieve datasets configurations for datasets '{dataset_identifiers}': {response_dict['message']}"
            )

        try:
            dataset_configs = DatasetConfigurationsDTO.model_validate(
                {"datasetConfigurations": response_dict.get("results", [])}
            )

            return dataset_configs
        except ValidationError as e:
            raise SodaCloudException(
                f"Failed to parse datasets configurations for datasets '{dataset_identifiers}': {str(e)}"
            ) from e

    def fetch_contract(
        self,
        dataset_identifier: DatasetIdentifier,
        contract_type: Optional[ContractType] = None,
        created_after: Optional[datetime] = None,
    ) -> str:
        filter_msg_list = []
        if contract_type is not None:
            filter_msg_list.append(f"contract type '{str(contract_type)}'")
        if created_after is not None:
            filter_msg_list.append(f"created after '{str(created_after)}'")
        filter_msg = f" with filter(s) {', '.join(filter_msg_list)}" if filter_msg_list else ""
        logger.info(
            f"{Emoticons.SCROLL} Fetching contract from Soda Cloud for dataset '{dataset_identifier.to_string()}'{filter_msg}"
        )
        and_expressions = [
            {
                "type": "equals",
                "left": {"type": "columnValue", "columnName": "identifier"},
                "right": {"type": "string", "value": dataset_identifier.to_string()},
            }
        ]
        if contract_type:
            and_expressions.append(
                {
                    "type": "equals",
                    "left": {"type": "columnValue", "columnName": "contractType"},
                    "right": {"type": "string", "value": contract_type.value},
                }
            )
        if created_after:
            and_expressions.append(
                {
                    "type": "greaterThanEqual",
                    "left": {"type": "columnValue", "columnName": "created"},
                    "right": {"type": "timestamp", "value": created_after},
                }
            )

        request = {"type": "sodaCoreContracts", "filter": {"type": "and", "andExpressions": and_expressions}}
        response = self._execute_query(request, request_log_name="fetch_contract")
        response_dict = response.json()

        if response.status_code != 200 or response_dict.get("results", None) is None:
            raise SodaCloudException(
                f"Failed to retrieve contract contents for dataset '{dataset_identifier.to_string()}'{filter_msg}: {response_dict['message']}"
            )

        results = response_dict.get("results")
        if len(results) == 0:
            raise SodaCloudException(f"No contract found for dataset '{dataset_identifier.to_string()}'{filter_msg}.")
        if len(results) > 1:
            raise SodaCloudException(
                f"Multiple contracts found for dataset '{dataset_identifier.to_string()}'{filter_msg} while expecting exactly one."
            )

        return results[0].get("contents")

    def poll_contract_skeleton_generation(
        self, dataset_identifier: DatasetIdentifier, blocking_timeout_in_minutes: int
    ) -> ContractSkeletonGenerationState:
        start_time = datetime.now(tz=timezone.utc)
        blocking_timeout = start_time + timedelta(minutes=blocking_timeout_in_minutes)
        attempt = 0
        time_to_wait_in_seconds: float = 5

        while datetime.now(tz=timezone.utc) < blocking_timeout:
            attempt += 1
            max_wait: timedelta = blocking_timeout - datetime.now(tz=timezone.utc)
            logger.debug(
                f"Asking Soda Cloud if a contract has been generated for dataset {dataset_identifier.to_string()} after {start_time}. Attempt {attempt}. Max wait: {max_wait}"
            )
            contract_skeleton_generation_state, next_poll_time_str = self._get_contract_skeleton_generation_state(
                dataset_identifier=dataset_identifier, created_after=start_time
            )

            if contract_skeleton_generation_state != ContractSkeletonGenerationState.PENDING:
                return contract_skeleton_generation_state

            if next_poll_time_str:
                logger.debug(
                    f"Soda Cloud suggested to ask contract skeleton generation for dataset {dataset_identifier.to_string()} status again at '{next_poll_time_str}' "
                    f"via header X-Soda-Next-Poll-Time"
                )
                next_poll_time: Optional[datetime] = convert_str_to_datetime(next_poll_time_str)
                if isinstance(next_poll_time, datetime):
                    now = datetime.now(tz=timezone.utc)
                    time_to_wait = next_poll_time - now
                    time_to_wait_in_seconds = time_to_wait.total_seconds()
                else:
                    time_to_wait_in_seconds = 60

            if time_to_wait_in_seconds > 0:
                logger.debug(
                    f"Sleeping {time_to_wait_in_seconds} seconds before asking "
                    f"Soda Cloud contract skeleton generation for dataset {dataset_identifier.to_string()} status again in ."
                )
                sleep(time_to_wait_in_seconds)

        raise FailedContractSkeletonGenerationException(
            f"Contract skeleton generation for dataset {dataset_identifier.to_string()} timed out after {blocking_timeout_in_minutes} minutes."
        )

    def _get_contract_skeleton_generation_state(
        self, dataset_identifier: DatasetIdentifier, created_after: datetime
    ) -> tuple[Optional[ContractSkeletonGenerationState], Optional[str]]:
        request = {
            "type": "sodaCoreContractSkeletonGenerationState",
            "datasetIdentifier": dataset_identifier.to_string(),
            "lastUpdatedAfter": created_after,
        }
        response = self._execute_query(request, request_log_name="get_contract_skeleton_generation_state")

        if response is None:
            logger.error("Failed to poll contract skeleton generation state.")
            return None, None

        logger.debug(f"Soda Cloud responded with {json.dumps(dict(response.headers))}\n{response.text}")
        if response.status_code != 200:
            error_details = response.json().get("message", response.text)
            raise SodaCloudException(error_details)

        response_body_dict: dict = response.json()
        if "state" not in response_body_dict:
            return None, response.headers.get("X-Soda-Next-Poll-Time", None)

        contract_skeleton_generation_state = ContractSkeletonGenerationState.from_value(response_body_dict["state"])
        logger.info(
            f"Contract skeleton generation for dataset {dataset_identifier.to_string()} has state '{contract_skeleton_generation_state}'"
        )

        return contract_skeleton_generation_state, response.headers.get("X-Soda-Next-Poll-Time", None)

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
            logger.debug(
                f"Sending {request_type} {request_log_name} to Soda Cloud with body: {self._clean_request_from_private_info(log_body_text)}"
            )
            response: Response = self._http_post(
                url=f"{self.api_url}/{request_type}",
                headers=self.headers,
                json=request_body,
                request_log_name=request_log_name,
            )

            trace_id: str = response.headers.get("X-Soda-Trace-Id", "N/A")
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
            elif not response.ok:
                try:
                    response_data = response.json()
                    message = response_data.get("message", "N/A")
                    code = response_data.get("code", "N/A")
                except json.JSONDecodeError:
                    # Fallback for non-JSON responses (e.g., HTML/plain-text error pages)
                    message = response.text
                    code = None

                logger.error(
                    f"Soda Cloud error for {request_type} '{request_log_name}':\n"
                    f"Status Code: {response.status_code}\n"
                    f"Message: '{message}' | Response Code: '{code}' | Trace Id: {trace_id}"
                )
                logger.debug(f"Response_text:\n{response.text}")
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

    def _clean_request_from_private_info(self, json_str: str) -> str:
        regex = re.compile(r'"token":\s*"[^"]+"')
        return regex.sub(r'"token": "****"', json_str)

    def _http_post(self, request_log_name: str = None, **kwargs) -> Response:
        return requests.post(**kwargs)

    def _execute_rest_get(self, relative_url_path: str, request_log_name: str, is_retry: bool = False) -> Response:
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

        if response.status_code == 401:
            logger.error(
                f"Soda Cloud authentication failed when executing GET '{url}'. Check your API key and secret. | "
                f"X-Soda-Trace-Id:{trace_id}"
            )
        elif not response.ok:
            logger.warning(
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

    def migration_create(self, v4_datasource_name: str, v3_dataset_ids: list[str]) -> str:
        logger.info(
            f"Creating migration to datasource '{v4_datasource_name}' for datasets with ids: {v3_dataset_ids[:10]}{'...' if len(v3_dataset_ids) > 10 else ''}"
        )

        request = {
            "type": "sodaCoreV3MigrationCreate",
            "v3DatasetIds": v3_dataset_ids,
            "v4DatasourceName": v4_datasource_name,
        }
        response = self._execute_command(request, request_log_name="create_migration")
        response_dict = response.json()

        if not response.ok or response_dict.get("v3MigrationId", None) is None:
            raise SodaCloudException(f"Failed to start migration.': {response_dict['message']}")

        migration_id = response_dict.get("v3MigrationId")
        logger.info(f"Migration created with id: {migration_id}")

        return migration_id

    def migration_upload_contracts(self, migration_id: str, contracts: list["ContractMigration"]) -> None:
        logger.info(f"Uploading {len(contracts)} contracts for migration {migration_id}")

        request = {
            "type": "sodaCoreV3MigrationUploadContracts",
            "v3MigrationId": migration_id,
            "generatedContracts": [c.model_dump(by_alias=True) for c in contracts],
        }
        response = self._execute_command(request, request_log_name="upload_migration_contracts")
        response.json()  # verify response is in JSON format

        if not response.ok:
            raise SodaCloudException(f"Failed to upload migration contracts: {response}")

        logger.info(f"Uploaded {len(contracts)} contracts for migration {migration_id}")

    def migration_poll_status(self, migration_id: str, blocking_timeout_in_minutes: int = 60) -> MigrationStatus:
        logger.info(f"Polling migration status for migration {migration_id}")

        blocking_timeout = datetime.now() + timedelta(minutes=blocking_timeout_in_minutes)
        attempt = 0
        while datetime.now() < blocking_timeout:
            attempt += 1
            max_wait: timedelta = blocking_timeout - datetime.now()
            logger.debug(f"Polling for migration '{migration_id}' status.  Attempt {attempt}. Max wait: {max_wait}")
            response = self._get_migration_status(migration_id)
            logger.debug(f"Migration status response: {json.dumps(response.text)}")
            if not response:
                logger.error(f"Failed to poll remote migration status. " f"Response: {response}")
                continue

            response_body_dict: Optional[dict] = response.json() if response else None

            if "migration" not in response_body_dict and "state" not in response["migration"]:
                logger.debug("Unable to parse the Cloud response. Retry in 5 seconds.")
                sleep(5)
                continue

            migration_state = MigrationStatus.from_value(response_body_dict["migration"]["state"])

            logger.info(f"Migration {migration_id} has state '{migration_state.value}'")

            if migration_state.is_publish_final_state:
                return migration_state

            time_to_wait_in_seconds: float = 5
            next_poll_time_str = response.headers.get("X-Soda-Next-Poll-Time")
            if next_poll_time_str:
                logger.debug(f"X-Soda-Next-Poll-Time: '{next_poll_time_str}'")
                next_poll_time: datetime = convert_str_to_datetime(next_poll_time_str)
                if isinstance(next_poll_time, datetime):
                    now = datetime.now(timezone.utc)
                    time_to_wait = next_poll_time - now
                    time_to_wait_in_seconds = time_to_wait.total_seconds()
                else:
                    time_to_wait_in_seconds = 60
            if time_to_wait_in_seconds > 0:
                logger.info(f"Polling for status in {time_to_wait_in_seconds} seconds.")
                sleep(time_to_wait_in_seconds)

    def _get_migration_status(self, migration_id: str) -> Response:
        return self._execute_query(
            request_log_name="migration_poll_status",
            query_json_dict={
                "type": "sodaCoreV3Migration",
                "v3MigrationId": migration_id,
            },
        )

    def migration_get_datasets(self, migration_id: str) -> list:
        logger.info(f"Getting datasets for migration {migration_id}")

        request = {
            "type": "sodaCoreV3DatasetMigrations",
            "filter": {
                "type": "equals",
                "left": {"type": "columnValue", "columnName": "v3MigrationId"},
                "right": {"type": "string", "value": migration_id},
            },
        }
        response = self._execute_query(request, request_log_name="get_migration_datasets")
        response_dict = response.json()

        if not response.ok or response_dict.get("results", None) is None:
            raise SodaCloudException(f"Failed to get migration datasets.': {response_dict['message']}")

        datasets_json = response_dict.get("results", [])

        return datasets_json

    def migration_fail_contract_generation(self, migration_id: str, error: str = "") -> None:
        logger.info(f"Failing contract generation for migration '{migration_id}'")

        request = {
            "type": "sodaCoreV3MigrationFailContractGeneration",
            "migrationId": migration_id,
            "error": {
                "message": error,
            },
        }
        response = self._execute_command(request, request_log_name="fail_contract_generation")
        response_dict = response.json()

        if not response.ok:
            raise SodaCloudException(f"Failed to fail contract generation: {response_dict['message']}")

        logger.info(f"Migration {migration_id} failed")

    def post_processing_update(
        self, stage: str, scan_id: str, state: PostProcessingStageState, error: Optional[str] = None
    ):
        logger.info(f"Updating post processing stage '{stage}' to state '{state.value}' for scan {scan_id}")

        request = {
            "type": "sodaCorePostProcessingUpdate",
            "scanId": scan_id,
            "name": stage,
            "state": state.value,
        }
        if error:
            request["error"] = error
        response = self._execute_command(request, request_log_name="post_processing_update")
        response.json()  # verify response is in JSON format

        if response.status_code != 200:
            raise SodaCloudException(f"Failed to update post processing stage: {response}")

        logger.info(f"Updated post processing stage '{stage}' to state '{state.value}' for scan {scan_id}")

    def logs_batch(self, scan_reference: str, body: str):
        headers = {
            "Authorization": self._get_token(),
            "Content-Type": "application/jsonlines",
        }

        response = self._http_post(
            url=f"{self.api_url}/logs/{scan_reference}/batchV3",
            headers=headers,
            data=body,
            request_log_name="logs_batch",
        )
        return response


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


def _build_post_processing_stages_dicts(
    contract_verification_result: ContractVerificationResult,
) -> list[Dict[str, str]]:
    if contract_verification_result and contract_verification_result.post_processing_stages:
        return [{"name": stage.name} for stage in contract_verification_result.post_processing_stages]
    else:
        return []


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
            "hasErrors": contract_verification_result.has_errors,
            "hasWarns": contract_verification_result.is_warned,
            "hasFailures": contract_verification_result.is_failed,
            "checks": _build_check_results_cloud_json_dicts(contract_verification_result),
            "logs": _build_log_cloud_json_dicts(contract_verification_result.log_records),
            "sourceOwner": "soda-core",
            "contract": _build_contract_cloud_json_dict(contract_verification_result.contract),
            "postProcessingStages": _build_post_processing_stages_dicts(contract_verification_result),
            "resultsIngestionMode": determine_verification_ingestion_mode(contract_verification_result).value,
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
        "checkPath": check_result.check.path,
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
        "outcome": check_outcome_to_soda_cloud(check_result.outcome),
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


def check_outcome_to_soda_cloud(outcome: CheckOutcome) -> str:
    if outcome == CheckOutcome.PASSED:
        return "pass"
    if outcome == CheckOutcome.WARN:
        return "warn"
    elif outcome == CheckOutcome.FAILED:
        return "fail"
    elif outcome == CheckOutcome.EXCLUDED:
        return "excluded"
    return "unevaluated"


def _build_diagnostics_json_dict(check_result: CheckResult) -> Optional[dict]:
    # if check_result.outcome == CheckOutcome.EXCLUDED:
    # return None

    return {
        #  TODO: this default 0 value is here only because check.diagnostics.value is a required non-nullable field in the api.
        "value": check_result.threshold_value or 0,
        "fail": _build_fail_threshold(check_result),
        "v4": _build_v4_diagnostics_check_type_json_dict(check_result),
    }


def _build_v4_diagnostics_check_type_json_dict(check_result: CheckResult) -> Optional[dict]:
    if check_result.outcome == CheckOutcome.EXCLUDED:
        # TODO: can we come up with any diagnostics for excluded checks?
        return None

    from soda_core.contracts.contract_interfaces import SodaCloudJsonable
    from soda_core.contracts.impl.check_types.freshness_check import (
        FreshnessCheckResult,
    )
    from soda_core.contracts.impl.check_types.schema_check import SchemaCheckResult

    if check_result.autogenerate_diagnostics_payload:
        return {
            "type": to_camel_case(check_result.check.type),
            **check_result.diagnostics_to_camel_case(),
        }
    elif check_result.check.type == "missing":
        return {
            "type": check_result.check.type,
            "failedRowsCount": check_result.diagnostic_metric_values.get("missing_count"),
            "failedRowsPercent": check_result.diagnostic_metric_values.get("missing_percent"),
            "datasetRowsTested": check_result.diagnostic_metric_values.get("dataset_rows_tested"),
            "checkRowsTested": check_result.diagnostic_metric_values.get("check_rows_tested"),
        }
    elif check_result.check.type == "invalid":
        return {
            "type": check_result.check.type,
            "failedRowsCount": check_result.diagnostic_metric_values.get("invalid_count"),
            "failedRowsPercent": check_result.diagnostic_metric_values.get("invalid_percent"),
            "datasetRowsTested": check_result.diagnostic_metric_values.get("dataset_rows_tested"),
            "checkRowsTested": check_result.diagnostic_metric_values.get("check_rows_tested"),
            "missingCount": check_result.diagnostic_metric_values.get("missing_count"),
        }
    elif check_result.check.type == "duplicate":
        return {
            "type": check_result.check.type,
            "failedRowsCount": check_result.diagnostic_metric_values.get("duplicate_count"),
            "failedRowsPercent": check_result.diagnostic_metric_values.get("duplicate_percent"),
            "datasetRowsTested": check_result.diagnostic_metric_values.get("dataset_rows_tested"),
            "checkRowsTested": check_result.diagnostic_metric_values.get("check_rows_tested"),
            "missingCount": check_result.diagnostic_metric_values.get("missing_count"),
        }
    elif check_result.check.type == "failed_rows":
        return {
            "type": check_result.check.type,
            "failedRowsCount": check_result.diagnostic_metric_values.get("failed_rows_count"),
            "failedRowsPercent": check_result.diagnostic_metric_values.get("failed_rows_percent"),
            "datasetRowsTested": check_result.diagnostic_metric_values.get("dataset_rows_tested"),
            "checkRowsTested": check_result.diagnostic_metric_values.get("check_rows_tested"),
        }
    elif check_result.check.type == "aggregate":
        return {
            "type": check_result.check.type,
            "datasetRowsTested": check_result.diagnostic_metric_values.get("dataset_rows_tested"),
            "checkRowsTested": check_result.diagnostic_metric_values.get("check_rows_tested"),
        }
    elif check_result.check.type == "metric":
        return {
            "type": check_result.check.type,
            "datasetRowsTested": check_result.diagnostic_metric_values.get("dataset_rows_tested"),
        }
    elif check_result.check.type == "row_count":
        return {
            "type": check_result.check.type,
            "checkRowsTested": check_result.diagnostic_metric_values.get("check_rows_tested"),
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
    else:
        # If we have a diagnostic metric values and it implements the ISodaCloudOutput interface, use it
        if check_result.diagnostic_metric_values is None:
            return None
        if isinstance(check_result.diagnostic_metric_values, SodaCloudJsonable):
            return check_result.diagnostic_metric_values.get_soda_cloud_output()

    raise SodaException(f"Unrecognized check result type: {type(check_result)}")


def _build_schema_column(column_metadata: ColumnMetadata) -> Optional[dict]:
    return {
        "name": column_metadata.column_name,
        # column_metadata.sql_data_type can be None when
        # expected columns of schema check don't include the type.
        # The type includes the data type parameters in round brackets eg VARCHAR(255)
        "type": (
            column_metadata.sql_data_type.get_sql_data_type_str_with_parameters()
            if column_metadata.sql_data_type
            else None
        ),
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
    elif scan_status in (RemoteScanStatus.COMPLETED_WITH_ERRORS,):
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


def _build_log_cloud_json_dicts(log_records: Optional[list[LogRecord]]) -> Optional[list[dict]]:
    if not log_records:
        return None
    return [build_log_cloud_json_dict(log_record, index) for index, log_record in enumerate(log_records)]


def build_log_cloud_json_dict(log_record: LogRecord, index: int) -> dict:
    return {
        "level": log_record.levelname.lower(),
        "message": log_record.getMessage(),
        "timestamp": datetime.fromtimestamp(log_record.created, tz=timezone.utc),
        "index": log_record.index if hasattr(log_record, "index") else index,
        "stage": log_record.stage if hasattr(log_record, "stage") else None,
        "doc": log_record.doc if hasattr(log_record, "doc") else None,
        "exception": log_record.exception if hasattr(log_record, "exception") else None,
        "thread": log_record.thread if hasattr(log_record, "thread") else None,
        "dataset": log_record.dataset if hasattr(log_record, "dataset") else None,
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


def determine_verification_ingestion_mode(
    contract_verification_result: ContractVerificationResult,
) -> VerificationIngestionMode:
    ingestion_mode = VerificationIngestionMode.FULL

    if any(
        check_result.outcome == CheckOutcome.EXCLUDED for check_result in contract_verification_result.check_results
    ):
        ingestion_mode = VerificationIngestionMode.PARTIAL

    return ingestion_mode
