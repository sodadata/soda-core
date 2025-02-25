from __future__ import annotations

import base64
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone, time
from decimal import Decimal
from enum import Enum
from tempfile import TemporaryFile
from time import sleep
from typing import Optional

import requests
from requests import Response

from soda_core.common.logs import Logs, Log, Emoticons
from soda_core.common.version import SODA_CORE_VERSION
from soda_core.common.yaml import YamlFileContent, YamlObject
from soda_core.contracts.contract_verification import ContractResult, \
    CheckResult, CheckOutcome, Threshold, Contract
from soda_core.contracts.impl.contract_yaml import ContractYaml


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


class SodaCloud:

    # Constants
    ORG_CONFIG_KEY_DISABLE_COLLECTING_WH_DATA = "disableCollectingWarehouseData"
    CSV_TEXT_MAX_LENGTH = 1500

    @classmethod
    def from_file(cls, soda_cloud_file_content: YamlFileContent) -> SodaCloud | None:

        logs = soda_cloud_file_content.logs

        if not isinstance(soda_cloud_file_content, YamlFileContent):
            logs.error(f"soda_cloud_file_content is not a YamlFileContent: {type(soda_cloud_file_content)}")
            return None

        if not soda_cloud_file_content.has_yaml_object():
            logs.error(f"Invalid Soda Cloud config file: No valid YAML object as file content")
            return None

        soda_cloud_yaml_root_object: YamlObject = soda_cloud_file_content.get_yaml_object()

        soda_cloud_yaml_object: YamlObject | None = soda_cloud_yaml_root_object.read_object_opt("soda_cloud")
        if not soda_cloud_yaml_object:
            logs.debug(f"key 'soda_cloud' is required in a Soda Cloud configuration file.")

        return SodaCloud(
            host=soda_cloud_yaml_object.read_string_opt(
                key="host",
                env_var="SODA_CLOUD_HOST",
                default_value="cloud.soda.io"
            ),
            api_key_id=soda_cloud_yaml_object.read_string(
                key="api_key_id",
                env_var="SODA_CLOUD_API_KEY_ID"
            ),
            api_key_secret=soda_cloud_yaml_object.read_string(
                key="api_key_secret",
                env_var="SODA_CLOUD_API_KEY_SECRET"
            ),
            token=soda_cloud_yaml_object.read_string_opt(
                key="token",
                env_var="SODA_CLOUD_TOKEN"
            ),
            port=soda_cloud_yaml_object.read_string_opt(
                key="port",
                env_var="SODA_CLOUD_PORT"
            ),
            scheme=soda_cloud_yaml_object.read_string_opt(
                key="scheme",
                env_var="SODA_CLOUD_SCHEME",
                default_value="https"
            ),
            logs=logs,
        )

    def __init__(
        self,
        host: str,
        api_key_id: str,
        api_key_secret: str,
        token: str | None,
        port: str | None,
        logs: Logs,
        scheme: str | None,
    ):
        self.host = host
        self.port = f":{port}" if port else ""
        self.scheme = scheme if scheme else "https"
        self.api_url = f"{self.scheme}://{self.host}{self.port}/api"
        self.api_key_id = api_key_id
        self.api_key_secret = api_key_secret
        self.token: str | None = token
        self.headers = {"User-Agent": f"SodaCore/{SODA_CORE_VERSION}"}
        self.logs = logs
        self.soda_cloud_trace_ids = {}
        self._organization_configuration = None

    def send_contract_result(self, contract_result: ContractResult, skip_publish: bool):
        contract_yaml_source_str = contract_result.contract.source.source_content_str
        self.logs.debug(f"Sending results to Soda Cloud {Emoticons.CLOUD}")
        soda_cloud_file_path : str = f"{contract_result.contract.soda_qualified_dataset_name.lower()}.yml"
        file_id: str | None = self._upload_contract(
            yaml_str_source=contract_yaml_source_str,
            soda_cloud_file_path=soda_cloud_file_path
        )
        if file_id:
            contract_result.contract.source.soda_cloud_file_id = file_id
            contract_result = self._build_contract_result_json(
                contract_result=contract_result, skip_publish=skip_publish
            )
            contract_result["type"] = "sodaCoreInsertScanResults"
            response: Response = self._execute_command(
                command_json_dict=contract_result,
                request_log_name="send_contract_verification_results"
            )
            if response.status_code == 200:
                self.logs.info(f"{Emoticons.OK_HAND} Results sent to Soda Cloud")
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} Contract wasn't uploaded so skipping "
                f"sending the results to Soda Cloud")

    def _build_contract_result_json(self, contract_result: ContractResult, skip_publish: bool) -> dict:
        check_result_cloud_json_dicts = [
            self._build_check_result_cloud_dict(check_result)
            for check_result in contract_result.check_results
            # TODO ask m1no if this should be ported
            # if check.check_type == CheckType.CLOUD
            # and (check.outcome is not None or check.force_send_results_to_cloud is True)
            # and check.archetype is None
        ]

        log_cloud_json_dicts: list[dict] = [
            self._build_log_cloud_json_dict(log, index)
            for index, log in enumerate(contract_result.logs.logs)
            # TODO ask m1no if this should be ported
            # if check.check_type == CheckType.CLOUD
            # and (check.outcome is not None or check.force_send_results_to_cloud is True)
            # and check.archetype is None
        ]

        querys = []
        # for query in contract_result._queries:
        #     query_list += query.get_cloud_dicts()

        contract_result_json: dict = self.to_jsonnable(  # type: ignore
            {
                "definitionName": contract_result.contract.soda_qualified_dataset_name,
                "defaultDataSource": contract_result.data_source_info.name,
                "defaultDataSourceProperties": {
                    "type": contract_result.data_source_info.type
                },
                # dataTimestamp can be changed by user, this is shown in Cloud as time of a scan.
                # It's the timestamp used to identify the time partition, which is the slice of data that is verified.
                "dataTimestamp": contract_result.data_timestamp,
                # scanStartTimestamp is the actual time when the scan started.
                "scanStartTimestamp": contract_result.started_timestamp,
                # scanEndTimestamp is the actual time when scan ended.
                "scanEndTimestamp": contract_result.ended_timestamp,
                "hasErrors": contract_result.logs.has_errors(),
                "hasWarnings": False,
                "hasFailures": contract_result.failed(),
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
                    "fileId": contract_result.contract.source.soda_cloud_file_id,
                    "dataset": {
                        "datasource": contract_result.contract.data_source_name,
                        "prefixes": contract_result.contract.dataset_prefix,
                        "name": contract_result.contract.dataset_name
                    },
                    "metadata": {
                        "source": {
                            "type": "local",
                            "filePath": contract_result.contract.source.local_file_path
                        }
                    }
                },
                "skipPublish": skip_publish
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
            "identities": {
                "vc1": check_result.check.identity
            },
            "name": check_result.check.name,
            "type": "generic",
            "definition": check_result.check.definition,
            "resourceAttributes": [], # TODO
            "location": {
                "filePath": (check_result.contract.source.local_file_path
                             if isinstance(check_result.contract.source.local_file_path, str) else "yamlstr.yml"),
                "line": check_result.check.contract_file_line,
                "col": check_result.check.contract_file_column
            },
            "dataSource": check_result.contract.data_source_name,
            "table": check_result.contract.dataset_name,
            "datasetPrefix" : check_result.contract.dataset_prefix,
            "column": check_result.check.column_name,
                # "metrics": [
                #         "metric-contract://SnowflakeCon_GLOBAL_BI_BUSINESS/GLOBAL_BI/BUSINESS/ORDERSCUBE-SnowflakeCon_GLOBAL_BI_BUSINESS-percentage_of_missing_orders > 100-7253408e"
                # ],
            "outcome": self._translate_check_outcome_for_soda_cloud(check_result.outcome),
            "source": "soda-contract"
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
                "fail": fail_threshold
            }
        return check_result_cloud_dict

    def _build_log_cloud_json_dict(self, log: Log, index: int) -> dict:
        return {
            "level": logging.getLevelName(log.level).lower(),
            "message": log.message,
            "timestamp": log.timestamp,
            "index": index,
        }

    def _upload_contract(self, yaml_str_source: str, soda_cloud_file_path: str) -> str | None:
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
                    "Content-Type": "application/octet-stream",
                    "Is-V3": "true",
                    "File-Path": soda_cloud_file_path,
                }

                if file_size_in_bytes == 0:
                    # because of https://github.com/psf/requests/issues/4215 we can't send content size
                    # when the size is 0 since requests blocks then on I/O indefinitely
                    self.logs.warning("Empty file upload detected, not sending Content-Length header")
                else:
                    headers["Content-Length"] = str(file_size_in_bytes)

                upload_response = self._http_post(url=f"{self.api_url}/scan/upload", headers=headers, data=temp_file)
                upload_response_json = upload_response.json()

                if isinstance(upload_response_json, dict) and "fileId" in upload_response_json:
                    return upload_response_json.get("fileId")
                else:
                    self.logs.error(
                        f"{Emoticons.POLICE_CAR_LIGHT} No fileId received in response: {upload_response_json}"
                    )
                    return None
        except Exception as e:
            self.logs.error(
                message=f"{Emoticons.POLICE_CAR_LIGHT} Soda cloud error: Could not upload contract "
                        f"to Soda Cloud: {e}",
                exception=e
            )

    def test_connection(self) -> Optional[str]:
        """
        Returns an error message or None if the connection test is successful
        """
        query: dict = {
            "type": "whoAmI"
        }
        self._execute_query(query_json_dict=query, request_log_name="who_am_i")
        if self.logs.has_errors():
            return self.logs.get_errors_str()
        else:
            return None

    def execute_contracts_on_agent(self, contract_yamls: list[ContractYaml]) -> list[ContractResult]:
        if not isinstance(contract_yamls, list) or len(contract_yamls) == 0:
            self.logs.info(f"No contracts to execute on Soda Agent")
            return []

        contract_yaml: ContractYaml = contract_yamls[0]
        contract_yaml_source_str: str = contract_yaml.contract_yaml_file_content.yaml_str_source
        contract_local_file_path: str | None = contract_yaml.contract_yaml_file_content.yaml_file_path
        data_source_name = contract_yaml.data_source
        dataset_prefix = contract_yaml.dataset_prefix
        dataset_name = contract_yaml.dataset

        soda_cloud_file_path : str = (
            contract_local_file_path if isinstance(contract_local_file_path, str) else "contract.yml"
        )
        file_id: str | None = self._upload_contract(
            yaml_str_source=contract_yaml_source_str,
            soda_cloud_file_path=soda_cloud_file_path
        )
        if not file_id:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} Contract wasn't uploaded so skipping "
                "sending the results to Soda Cloud"
            )
            return []

        verify_contract_command: dict = {
            "type": "sodaCoreVerifyContract",
            "contract": {
                "fileId": file_id,
                "dataset": {
                    "datasource": data_source_name,
                    "prefixes": dataset_prefix,
                    "name": dataset_name
                },
                "metadata": {
                    "source": {
                        "type": "local",
                        "filePath": contract_local_file_path
                    }
                }
            }
        }
        response: Response = self._execute_command(
            command_json_dict=verify_contract_command,
            request_log_name="verify_contract"
        )
        response_json: dict = response.json()
        scan_id: str = response_json.get("scanId")

        scan_is_finished: bool = self._poll_remote_scan_finished(scan_id=scan_id)

        self.logs.debug(f"Asking Soda Cloud the logs of scan {scan_id}")
        logs_response: Response = self._get_scan_logs(scan_id=scan_id)
        self.logs.debug(f"Soda Cloud responded with {json.dumps(dict(logs_response.headers))}\n{logs_response.text}")

        response_json: dict = logs_response.json()
        logs: list[dict] = response_json.get("content")
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
        if isinstance(logs, list):
            for log in logs:
                if isinstance(log, dict):
                    json_level_str: str = log.get("level")
                    logging_level: int = logging.getLevelName(json_level_str.upper())
                    timestamp_str: str = log.get("timestamp")
                    timestamp: datetime = self.convert_str_to_datetime(timestamp_str)
                    self.logs.log(
                        Log(level=logging_level,
                            message=log.get("message"),
                            timestamp=timestamp,
                            index=log.get("index")
                        )
                    )
                else:
                    self.logs.debug(f"Expected dict for logs list element, but was {type(log).__name__}")
        elif logs is None:
            self.logs.debug(f"No logs in Soda Cloud response")
        else:
            self.logs.debug(f"Expected dict for logs, but was {type(logs).__name__}")

        if not scan_is_finished:
            self.logs.error(f"{Emoticons.POLICE_CAR_LIGHT} Max retries exceeded. "
                            f"Contract verification did not finish yet.")

        return [ContractResult(
            contract=Contract(
                data_source_name=data_source_name,
                dataset_prefix=dataset_prefix,
                dataset_name=dataset_name,
                soda_qualified_dataset_name=None,
                source=None
            ),
            data_source_info=None,
            data_timestamp=None,
            started_timestamp=None,
            ended_timestamp=None,
            measurements=[],
            check_results=[
                # TODO
            ],
            logs=self.logs
        )]

    def _poll_remote_scan_finished(self, scan_id: str, max_retry: int = 5) -> bool:
        attempt = 0
        while attempt < max_retry:
            attempt += 1

            self.logs.debug(f"Asking Soda Cloud if scan {scan_id} is already completed. Attempt {attempt}/{max_retry}.")
            response = self._get_scan_status(scan_id)
            self.logs.debug(f"Soda Cloud responded with {json.dumps(dict(response.headers))}\n{response.text}")
            if response:
                response_body_dict: Optional[dict] = response.json() if response else None
                scan_status: str = response_body_dict.get("state") if response_body_dict else None

                self.logs.debug(f"Scan {scan_id} status is '{scan_status}'")

                if scan_status in REMOTE_SCAN_FINAL_STATES:
                    return True

                if attempt < max_retry:
                    time_to_wait_in_seconds: float = 5
                    next_poll_time_str = response.headers.get("X-Soda-Next-Poll-Time")
                    if next_poll_time_str:
                        self.logs.debug(
                            f"Soda Cloud suggested to ask scan {scan_id} status again at '{next_poll_time_str}' "
                            f"via header X-Soda-Next-Poll-Time"
                        )
                        next_poll_time: datetime = self.convert_str_to_datetime(next_poll_time_str)
                        now = datetime.now(timezone.utc)
                        time_to_wait = next_poll_time - now
                        time_to_wait_in_seconds = time_to_wait.total_seconds()
                    if time_to_wait_in_seconds > 0:
                        self.logs.debug(
                            f"Sleeping {time_to_wait_in_seconds} seconds before asking "
                            f"Soda Cloud scan {scan_id} status again in ."
                        )
                        sleep(time_to_wait_in_seconds)
            else:
                self.logs.error(f"{Emoticons.POLICE_CAR_LIGHT} Failed to poll remote scan status. "
                                f"Response: {response}")

        return False

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
            request_type="query",
            request_log_name=request_log_name,
            request_body=query_json_dict,
            is_retry=True
        )

    def _execute_command(self, command_json_dict: dict, request_log_name: str) -> Response:
        return self._execute_cqrs_request(
            request_type="command",
            request_log_name=request_log_name,
            request_body=command_json_dict,
            is_retry=True
        )

    def _execute_cqrs_request(
        self,
        request_type: str,
        request_log_name: str,
        request_body: dict,
        is_retry: bool
    ) -> Response:
        try:
            request_body["token"] = self._get_token()
            log_body_text: str = json.dumps(self.to_jsonnable(request_body), indent=2)
            self.logs.debug(f"Sending {request_type} {request_log_name} to Soda Cloud with body: {log_body_text}")
            response: Response = self._http_post(
                url=f"{self.api_url}/{request_type}", headers=self.headers, json=request_body, request_log_name=request_log_name
            )

            trace_id: str = response.headers.get("X-Soda-Trace-Id")
                # TODO let m1no check if this is still needed...
                # if request_name:
                #     trace_id = response.headers.get("X-Soda-Trace-Id")
                #
                #     if trace_id:
                #         self.soda_cloud_trace_ids[request_name] = trace_id

            if response.status_code == 401 and is_retry:
                self.logs.debug(
                    f"Soda Cloud authentication failed for {request_type} {request_log_name}. "
                    f"Probably token expired. Re-authenticating... | X-Soda-Trace-Id:{trace_id}"
                )
                self.token = None
                response = self._execute_cqrs_request(
                    request_type=request_type,
                    request_log_name=request_log_name,
                    request_body=request_body,
                    is_retry=False
                )
            elif response.status_code != 200:
                self.logs.error(
                    f"{Emoticons.POLICE_CAR_LIGHT} Soda Cloud error for {request_type} {request_log_name} | status_code:{response.status_code} | "
                    f"X-Soda-Trace-Id:{trace_id} | response_text:{response.text}"
                )
            else:
                self.logs.debug(
                    f"{Emoticons.OK_HAND} Soda Cloud {request_type} {request_log_name} OK | X-Soda-Trace-Id:{trace_id}"
                )

            return response
        except Exception as e:
            self.logs.error(f"{Emoticons.POLICE_CAR_LIGHT} Error while executing Soda Cloud {request_type} {request_log_name}", exception=e)

    def _http_post(self, request_log_name: str = None, **kwargs) -> Response:
        return requests.post(**kwargs)

    def _execute_rest_get(
        self,
        relative_url_path: str,
        request_log_name: str,
        is_retry: bool = True
    ) -> Response:

        credentials_plain = f"{self.api_key_id}:{self.api_key_secret}"
        credentials_encoded = base64.b64encode(credentials_plain.encode()).decode()

        headers = {
            "Authorization": f"Basic {credentials_encoded}",
            "Content-Type": "application/octet-stream", # Probably not needed
            "Is-V3": "true", # Probably not needed
            "Accept": "application/json",
        }

        url: str = f"{self.api_url}/v1/{relative_url_path}"
        self.logs.debug(f"Sending GET {url} request to Soda Cloud")
        response: Response = self._http_get(
            url=url,
            headers=headers
        )

        trace_id: str = response.headers.get("X-Soda-Trace-Id")

        if response.status_code == 401 and is_retry:
            self.logs.debug(
                f"Soda Cloud authentication failed. Probably token expired. Re-authenticating... | "
                f"X-Soda-Trace-Id:{trace_id}"
            )
            self.token = None
            response = self._execute_rest_get(
                relative_url_path=relative_url_path,
                request_log_name=request_log_name,
                is_retry=False
            )
        elif response.status_code != 200:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} Soda Cloud error for {request_log_name} | status_code:{response.status_code} | "
                f"X-Soda-Trace-Id:{trace_id} | response_text:{response.text}"
            )
        else:
            self.logs.debug(
                f"{Emoticons.OK_HAND} Soda Cloud {request_log_name} OK | X-Soda-Trace-Id:{trace_id}"
            )

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
            return SodaCloud.convert_datetime_to_str(o)
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

    @classmethod
    def convert_datetime_to_str(cls, dt: datetime) -> str:
        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")

    @classmethod
    def convert_str_to_datetime(cls, date_string: str) -> datetime:
        # fromisoformat raises ValueError if date_string ends with a Z:
        # Eg Invalid isoformat string: '2025-02-21T06:16:59Z'
        if date_string.endswith("Z"):
            # Z means Zulu time, which is UTC
            # Converting timezone to format that fromisoformat understands
            datetime_str_without_z: str = date_string[:-1]
            datetime_str_without_z_seconds = re.sub(r"\.(\d+)$", "", datetime_str_without_z)
            date_string = f"{datetime_str_without_z_seconds}+00:00"
        return datetime.fromisoformat(date_string)
