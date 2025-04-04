from __future__ import annotations

import base64
import json
import logging
import os
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from logging import LogRecord
from soda_core.contracts.contract import ContractIdentifier
from tempfile import TemporaryFile
from time import sleep
from typing import Optional

import requests
from requests import Response
from soda_core.common.datetime_conversions import (
    convert_datetime_to_str,
    convert_str_to_datetime,
)
from soda_core.common.logging_constants import Emoticons, ExtraKeys, soda_logger
from soda_core.common.logs import Location, Logs
from soda_core.common.version import SODA_CORE_VERSION
from soda_core.common.yaml import YamlObject, YamlSource
from soda_core.contracts.contract_publication import ContractPublicationResult
from soda_core.contracts.contract_verification import (
    Check,
    CheckOutcome,
    CheckResult,
    Contract,
    ContractVerificationResult,
    Threshold,
    YamlFileContentInfo,
)
from soda_core.contracts.impl.contract_yaml import ContractYaml

logger: logging.Logger = soda_logger


class RemoteScanStatus:
    QUEUING = "queuing"
    EXECUTING = "executing"
    CANCELATION_REQUESTED = "cancelationRequested"
    TIME_OUT_REQUESTED = "timeOutRequested"
    CANCELED = "canceled"
    TIMED_OUT = "timedOut"
    FAILED = "failed"
    COMPLETED_WITH_ERRORS = "completedWithErrors"
    COMPLETED_WITH_FAILURES = "completedWithFailures"
    COMPLETED_WITH_WARNINGS = "completedWithWarnings"
    COMPLETED = "completed"


REMOTE_SCAN_FINAL_STATES = [
    RemoteScanStatus.CANCELED,
    RemoteScanStatus.TIMED_OUT,
    RemoteScanStatus.FAILED,
    RemoteScanStatus.COMPLETED_WITH_ERRORS,
    RemoteScanStatus.COMPLETED_WITH_FAILURES,
    RemoteScanStatus.COMPLETED_WITH_WARNINGS,
    RemoteScanStatus.COMPLETED,
]


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
    def from_yaml_source(
        cls, soda_cloud_yaml_source: YamlSource, variables: Optional[dict[str, str]]
    ) -> Optional[SodaCloud]:
        soda_cloud_yaml_source.set_file_type("Soda Cloud")
        soda_cloud_yaml_source.resolve(variables=variables)
        soda_cloud_yaml_root_object: YamlObject = soda_cloud_yaml_source.parse()

        if not soda_cloud_yaml_root_object:
            logger.error(f"Invalid Soda Cloud config file: No valid YAML object as file content")
            return None

        soda_cloud_yaml_object: Optional[YamlObject] = soda_cloud_yaml_root_object.read_object_opt("soda_cloud")
        if not soda_cloud_yaml_object:
            logger.debug(f"key 'soda_cloud' is required in a Soda Cloud configuration file.")

        return SodaCloud(
            host=soda_cloud_yaml_object.read_string_opt(key="host", default_value="cloud.soda.io"),
            api_key_id=soda_cloud_yaml_object.read_string(key="api_key_id"),
            api_key_secret=soda_cloud_yaml_object.read_string(key="api_key_secret"),
            token=soda_cloud_yaml_object.read_string_opt(key="token"),
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

    def upload_contract_file(self, contract: Contract) -> str:
        contract_yaml_source_str = contract.source.source_content_str
        logger.debug(f"Sending results to Soda Cloud {Emoticons.CLOUD}")
        soda_cloud_file_path: str = f"{contract.soda_qualified_dataset_name.lower()}.yml"
        file_id: Optional[str] = self._upload_contract(
            yaml_str_source=contract_yaml_source_str, soda_cloud_file_path=soda_cloud_file_path
        )
        if file_id:
            contract.source.soda_cloud_file_id = file_id

        return file_id

    def send_contract_result(
        self, contract_verification_result: ContractVerificationResult
    ) -> bool:
        """
        Returns True if a 200 OK was received, False otherwise
        """
        contract_verification_result = self._build_contract_result_json(
            contract_verification_result=contract_verification_result
        )
        contract_verification_result["type"] = "sodaCoreInsertScanResults"
        response: Response = self._execute_command(
            command_json_dict=contract_verification_result, request_log_name="send_contract_verification_results"
        )
        if response.status_code == 200:
            logger.info(f"{Emoticons.OK_HAND} Results sent to Soda Cloud")
            response_json = response.json()
            if isinstance(response_json, dict):
                cloud_url: Optional[str] = response_json.get("cloudUrl")
                if isinstance(cloud_url, str):
                    logger.info(f"To view the dataset on Soda Cloud, see {cloud_url}")
            return True
        else:
            return False

    def _build_contract_result_json(
        self, contract_verification_result: ContractVerificationResult
    ) -> dict:
        check_result_cloud_json_dicts = [
            self._build_check_result_cloud_dict(check_result)
            for check_result in contract_verification_result.check_results
            # TODO ask m1no if this should be ported
            # if check.check_type == CheckType.CLOUD
            # and (check.outcome is not None or check.force_send_results_to_cloud is True)
            # and check.archetype is None
        ]

        log_cloud_json_dicts: list[dict] = [
            self._build_log_cloud_json_dict(log_record, index)
            for index, log_record in enumerate(contract_verification_result.log_records)
            # TODO ask m1no if this should be ported
            # if check.check_type == CheckType.CLOUD
            # and (check.outcome is not None or check.force_send_results_to_cloud is True)
            # and check.archetype is None
        ]

        contract_result_json: dict = self.to_jsonnable(  # type: ignore
            {
                "definitionName": contract_verification_result.contract.soda_qualified_dataset_name,
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
                # "metrics": [metric.get_cloud_dict() for metric in contract_result._metrics],
                # If archetype is not None, it means that check is automated monitoring
                "checks": check_result_cloud_json_dicts,
                # "queries": querys,
                # "automatedMonitoringChecks": automated_monitoring_checks,
                # "profiling": profiling,
                # "metadata": [
                #     discover_tables_result.get_cloud_dict()
                #     for discover_tables_result in contract_result._discover_tables_result_tables
                # ],
                "logs": log_cloud_json_dicts,
                "sourceOwner": "soda-core",
                "contract": {
                    "fileId": contract_verification_result.contract.source.soda_cloud_file_id,
                    "dataset": {
                        "datasource": contract_verification_result.contract.data_source_name,
                        "prefixes": contract_verification_result.contract.dataset_prefix,
                        "name": contract_verification_result.contract.dataset_name,
                    },
                    "metadata": {
                        "source": {
                            "type": "local",
                            "filePath": contract_verification_result.contract.source.local_file_path,
                        }
                    },
                },
            }
        )
        soda_scan_id: Optional[str] = os.environ.get("SODA_SCAN_ID")
        if soda_scan_id:
            contract_result_json["scanId"] = soda_scan_id
        return contract_result_json

    def _translate_check_outcome_for_soda_cloud(self, outcome: CheckOutcome) -> str:
        if outcome == CheckOutcome.PASSED:
            return "pass"
        elif outcome == CheckOutcome.FAILED:
            return "fail"
        return "unevaluated"

    def _build_check_result_cloud_dict(self, check_result: CheckResult) -> dict:
        check_result_cloud_dict: dict = {
            "identities": {"vc1": check_result.check.identity},
            "checkPath": self._build_check_path(check_result),
            "name": check_result.check.name,
            "type": "generic",
            "checkType": check_result.check.type,
            "definition": check_result.check.definition,
            "resourceAttributes": [],  # TODO
            "location": {
                "filePath": (
                    check_result.contract.source.local_file_path
                    if isinstance(check_result.contract.source.local_file_path, str)
                    else "yamlstr.yml"
                ),
                "line": check_result.check.contract_file_line,
                "col": check_result.check.contract_file_column,
            },
            "dataSource": check_result.contract.data_source_name,
            "table": check_result.contract.dataset_name,
            "datasetPrefix": check_result.contract.dataset_prefix,
            "column": check_result.check.column_name,
            # "metrics": [
            #         "metric-contract://SnowflakeCon_GLOBAL_BI_BUSINESS/GLOBAL_BI/BUSINESS/ORDERSCUBE-SnowflakeCon_GLOBAL_BI_BUSINESS-percentage_of_missing_orders > 100-7253408e"
            # ],
            "outcome": self._translate_check_outcome_for_soda_cloud(check_result.outcome),
            "source": "soda-contract",
        }
        if check_result.metric_value is not None and check_result.check.threshold is not None:
            t: Threshold = check_result.check.threshold
            fail_threshold: dict = {}
            if t.must_be_less_than_or_equal is not None:
                fail_threshold["greaterThan"] = t.must_be_less_than_or_equal
            if t.must_be_less_than is not None:
                fail_threshold["greaterThanOrEqual"] = t.must_be_less_than
            if t.must_be_greater_than_or_equal is not None:
                fail_threshold["lessThan"] = t.must_be_greater_than_or_equal
            if t.must_be_greater_than is not None:
                fail_threshold["lessThanOrEqual"] = t.must_be_greater_than
            check_result_cloud_dict["diagnostics"] = {
                "blocks": [],
                "value": check_result.metric_value,
                "fail": fail_threshold,
            }
        return check_result_cloud_dict

    def _build_check_path(self, check_result: CheckResult) -> str:
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

    def _build_log_cloud_json_dict(self, log_record: LogRecord, index: int) -> dict:
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

    def _upload_contract(self, yaml_str_source: str, soda_cloud_file_path: str) -> Optional[str]:
        """
        Returns a Soda Cloud fileId or None if something is wrong.
        """
        try:
            with TemporaryFile() as temp_file:
                rows_json_bytes = bytearray(yaml_str_source, "utf-8")
                temp_file.write(rows_json_bytes)

                file_size_in_bytes = temp_file.tell()
                temp_file.seek(0)

                headers = {
                    "Authorization": self._get_token(),
                    "Content-Type": "application/yaml",
                    "Is-V3": "true",
                    "File-Path": soda_cloud_file_path,
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
        data_source_name = contract_yaml.data_source
        dataset_prefix = contract_yaml.dataset_prefix
        dataset_name = contract_yaml.dataset

        can_publish_and_verify, reason = self.can_publish_and_verify_contract(
            data_source_name, dataset_prefix, dataset_name
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
        file_id: Optional[str] = self._upload_contract(
            yaml_str_source=contract_yaml_str_original, soda_cloud_file_path=soda_cloud_file_path
        )
        if not file_id:
            logger.critical("Uploading the contract file failed")
            return ContractPublicationResult(contract=None)

        publish_contract_command: dict = {
            "type": "sodaCorePublishContract",
            "contract": {
                "fileId": file_id,
                "dataset": {"datasource": data_source_name, "prefixes": dataset_prefix, "name": dataset_name},
                "metadata": {"source": {"type": "local", "filePath": contract_local_file_path}},
            },
        }
        response: Response = self._execute_command(
            command_json_dict=publish_contract_command, request_log_name="publish_contract"
        )

        if response.status_code == 200:
            logger.info(f"{Emoticons.OK_HAND} Contract published on Soda Cloud")
        else:
            logger.critical(f"Failed ot publish on Soda Cloud")

        response_json = response.json()
        source_metadata = (
            response_json["metadata"]["source"]
            if "metadata" in response_json and "source" in response_json["metadata"]
            else {}
        )
        yaml_file_path = source_metadata["filePath"] if "filePath" in source_metadata else None

        return ContractPublicationResult(
            contract=Contract(
                data_source_name=data_source_name,
                dataset_prefix=dataset_prefix,
                dataset_name=dataset_name,
                source=YamlFileContentInfo(
                    local_file_path=yaml_file_path,
                    source_content_str=None,
                    soda_cloud_file_id=response_json.get("fileId", None),
                ),
                soda_qualified_dataset_name=None,
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
        self, contract_yaml: ContractYaml, blocking_timeout_in_minutes: int
    ) -> ContractVerificationResult:
        contract_yaml_str_original: str = contract_yaml.contract_yaml_source.yaml_str_original
        contract_local_file_path: Optional[str] = contract_yaml.contract_yaml_source.file_path
        data_source_name = contract_yaml.data_source
        dataset_prefix = contract_yaml.dataset_prefix
        dataset_name = contract_yaml.dataset

        can_publish_and_verify, reason = self.can_publish_and_verify_contract(
            data_source_name, dataset_prefix, dataset_name
        )
        if not can_publish_and_verify:
            if reason is None:
                logger.error(f"Skipping contract verification because of an error (see logs)")
            else:
                logger.error(f"Skipping contract verification because of insufficient permissions: {reason}")
            return ContractVerificationResult(
                contract=Contract(
                    data_source_name=data_source_name,
                    dataset_prefix=dataset_prefix,
                    dataset_name=dataset_name,
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
                sending_results_to_soda_cloud_failed=True,
                log_records=None,
            )

        soda_cloud_file_path: str = (
            contract_local_file_path if isinstance(contract_local_file_path, str) else "contract.yml"
        )
        file_id: Optional[str] = self._upload_contract(
            yaml_str_source=contract_yaml_str_original, soda_cloud_file_path=soda_cloud_file_path
        )
        if not file_id:
            logger.critical(f"Contract wasn't uploaded so skipping " "sending the results to Soda Cloud")
            return []

        verify_contract_command: dict = {
            "type": "sodaCoreVerifyContract",
            "contract": {
                "fileId": file_id,
                "dataset": {"datasource": data_source_name, "prefixes": dataset_prefix, "name": dataset_name},
                "metadata": {"source": {"type": "local", "filePath": contract_local_file_path}},
            },
        }
        response: Response = self._execute_command(
            command_json_dict=verify_contract_command, request_log_name="verify_contract"
        )
        response_json: dict = response.json()
        scan_id: str = response_json.get("scanId")

        log_records: Optional[list[LogRecord]] = None
        if response.status_code == 200 and scan_id:
            scan_is_finished, contract_dataset_cloud_url = self._poll_remote_scan_finished(
                scan_id=scan_id, blocking_timeout_in_minutes=blocking_timeout_in_minutes
            )

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

            if contract_dataset_cloud_url:
                logger.info(f"See contract dataset on Soda Cloud: {contract_dataset_cloud_url}")

            logs.remove_from_root_logger()
            log_records = logs.records

        return ContractVerificationResult(
            contract=Contract(
                data_source_name=data_source_name,
                dataset_prefix=dataset_prefix,
                dataset_name=dataset_name,
                soda_qualified_dataset_name=None,
                source=None,
            ),
            data_source=None,
            data_timestamp=None,
            started_timestamp=None,
            ended_timestamp=None,
            measurements=[],
            check_results=[
                # TODO
            ],
            sending_results_to_soda_cloud_failed=True,
            log_records=log_records,
        )

    def fetch_contract_for_dataset(self, dataset_identifier: str) -> Optional[str]:
        """Fetch the contract contents for the given dataset identifier.

        Returns:
            The contract content as a string, or None if:
            - the data source or dataset does not exist
            - no contract is linked to the dataset
            - an unexpected response is received from the backend
        """

        logger.info(f"{Emoticons.SCROLL} Fetching contract from Soda Cloud for dataset {dataset_identifier}")
        parsed_identifier = ContractIdentifier.parse(dataset_identifier)
        request = {"type": "sodaCoreContractFile", "dataset": {
            "datasource": parsed_identifier.data_source,
            "prefixes": parsed_identifier.prefixes,
            "name": parsed_identifier.dataset,
        }}
        response = self._execute_query(request, request_log_name="get_contract_file")

        if response.status_code != 200:
            logger.error(f"{Emoticons.CROSS_MARK} Failed to retrieve contract contents for dataset '{dataset_identifier}'")
            return None

        response_dict = response.json()
        return response_dict.get("contents")


    def _poll_remote_scan_finished(self, scan_id: str, blocking_timeout_in_minutes: int) -> tuple[bool, Optional[str]]:
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
            if response:
                response_body_dict: Optional[dict] = response.json() if response else None
                scan_state: str = response_body_dict.get("state") if response_body_dict else None
                contract_dataset_cloud_url: Optional[str] = (
                    response_body_dict.get("contractDatasetCloudUrl") if response_body_dict else None
                )

                logger.info(f"Scan {scan_id} has state '{scan_state}'")

                if scan_state in REMOTE_SCAN_FINAL_STATES:
                    return True, contract_dataset_cloud_url

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
            else:
                logger.error(f"Failed to poll remote scan status. " f"Response: {response}")

        return False, None

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
            log_body_text: str = json.dumps(self.to_jsonnable(request_body), indent=2)
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
                logger.critical(
                    f"Soda Cloud error for {request_type} {request_log_name} | status_code:{response.status_code} | "
                    f"X-Soda-Trace-Id:{trace_id} | response_text:{response.text}"
                )
            else:
                logger.debug(
                    f"{Emoticons.OK_HAND} Soda Cloud {request_type} {request_log_name} OK | X-Soda-Trace-Id:{trace_id}"
                )

            return response
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
            logger.critical(
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
                raise AssertionError(f"Soda Cloud login failed {login_response.status_code}. Check credentials.")
            login_response_json = login_response.json()

            self.token = login_response_json.get("token")
            assert self.token, "No token in login response?!"
        return self.token

    @classmethod
    def to_jsonnable(cls, o) -> object:
        if o is None or isinstance(o, str) or isinstance(o, int) or isinstance(o, float) or isinstance(o, bool):
            return o
        if isinstance(o, dict):
            for key, value in o.items():
                update = False
                if not isinstance(key, str):
                    del o[key]
                    key = str(key)
                    update = True

                jsonnable_value = cls.to_jsonnable(value)
                if value is not jsonnable_value:
                    value = jsonnable_value
                    update = True
                if update:
                    o[key] = value
            return o
        if isinstance(o, tuple):
            return cls.to_jsonnable(list(o))
        if isinstance(o, list):
            for i in range(len(o)):
                element = o[i]
                jsonnable_element = cls.to_jsonnable(element)
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
