from __future__ import annotations

import json
import logging
import re
from abc import ABC
from dataclasses import dataclass
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
    CheckResult, CheckOutcome, Threshold


class RemoteScanStatus(Enum):
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
            logs.error(f"key 'soda_cloud' is required in a Soda Cloud configuration file")
            return None

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
        contract_yaml_source_str = contract_result.contract_info.source.source_content_str
        self.logs.debug(f"Sending results to Soda Cloud {Emoticons.CLOUD}")
        soda_cloud_file_path : str = f"{contract_result.contract_info.soda_qualified_dataset_name.lower()}.yml"
        file_id: str | None = self._upload_contract(
            yaml_str_source=contract_yaml_source_str,
            soda_cloud_file_path=soda_cloud_file_path
        )
        if file_id:
            contract_result.contract_info.source.soda_cloud_file_id = file_id
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
            self.logs.error("Contract wasn't uploaded so skipping sending the results to Soda Cloud")

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

        return self.to_jsonnable(  # type: ignore
            {
                "definitionName": contract_result.soda_qualified_dataset_name,
                "defaultDataSource": contract_result.data_source_name,
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
                    "fileId": contract_result.contract_info.source.soda_cloud_file_id,
                    "dataset": {
                        "datasource": contract_result.contract_info.data_source_name,
                        "prefixes": contract_result.contract_info.dataset_prefix,
                        "name": contract_result.contract_info.dataset_name
                    },
                    "metadata": {
                        "source": {
                            "type": "local",
                            "filePath": contract_result.contract_info.source.local_file_path
                        }
                    }
                },
                "skipPublish": skip_publish
            }
        )

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
                "filePath": check_result.contract.source.local_file_path,
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
                    self.logs.error(f"No fileId received in response: {upload_response_json}")
                    return None
        except Exception as e:
            self.logs.error(f"Soda cloud error: Could not upload contract: {e}", exception=e)

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

    def verify_on_agent(
        self,
        contract_yaml_source_str: str,
        contract_local_file_path: str | None,
        data_source_name: str,
        dataset_prefix: list[str],
        dataset_name: str
    ) -> list[Log]:
        soda_cloud_file_path : str = (
            contract_local_file_path if isinstance(contract_local_file_path, str) else "contract.yml"
        )
        file_id: str | None = self._upload_contract(
            yaml_str_source=contract_yaml_source_str,
            soda_cloud_file_path=soda_cloud_file_path
        )
        if not file_id:
            self.logs.error("Contract wasn't uploaded so skipping sending the results to Soda Cloud")
            return None

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

        response: Response = self._get_scan_logs(scan_id=scan_id)
        logs: list[dict] = response.json()
        self.logs.info("Logs from contract verification on Soda Agent:")
        for log in logs:
            self.logs.log(
                Log(level=log.get("level"),
                    message=log.get("message"),
                )
            )

        if not scan_is_finished:
            self.logs.error("Max retries exceeded. Contract verification did not finish yet.")

    def _poll_remote_scan_finished(self, scan_id: str, poll_wait: int = 5, max_retry: int = 100) -> bool:
        pass

        result: Optional[str] = None
        attempt = 0
        while attempt < max_retry:
            attempt += 1

            self.logs.debug(f"Polling remote scan result attempt {attempt}/{max_retry}.")
            response = self._get_remote_scan_status(scan_id)
            if response:
                next_poll_time = response.headers.get("X-Soda-Next-Poll-Time")
                if next_poll_time:
                    poll_wait = (self._datetime_from_iso_zulu(next_poll_time) - datetime.now(timezone.utc)).seconds
                self.logs.debug(f"Next poll in {poll_wait} seconds.")

                json: Optional[dict] = response.json() if response else None
                scan_status: Optional[dict] = json.get("scanStatus") if json else None
                scan_status_value: Optional[str] = scan_status.get("value") if scan_status else None

                self.logs.debug(f"Remote scan status: '{scan_status_value}'.")

                if scan_status_value in REMOTE_SCAN_FINAL_STATES:
                    return True

                if attempt == max_retry:
                    raise Exception(
                        f"Remote scan did not finish in {max_retry} attempts. Last status: {scan_status_value}"
                    )
                else:
                    sleep(poll_wait)
            else:
                raise Exception(f"Failed to poll remote scan status. Response: {response}")

        return False

    def _datetime_from_iso_zulu(self, date_string: str) -> datetime:
        if date_string.endswith("Z"):
            date_string = date_string[:-1]
        return datetime.fromisoformat(date_string)

    def _get_remote_scan_status(self, scan_id: str) -> Response:
        return self._execute_rest_get(
            relative_url_path=f"v1/scans/{scan_id}",
            request_log_name="remote_scan_poll_result",
        )

    def _get_scan_logs(self, scan_id: str) -> Response:
        return self._execute_rest_get(
            relative_url_path=f"v1/scans/{scan_id}/logs",
            request_log_name="remote_scan_poll_result",
        )

    # @staticmethod
    # def _serialize_file_upload_value(value):
    #     if value is None or isinstance(value, str) or isinstance(value, int) or isinstance(value, float):
    #         return value
    #     return str(value)

    def _fileify(self, name: str):
        return re.sub(r"\W+", "_", name).lower()

    def get_historic_data(self, historic_descriptor: HistoricDescriptor):
        measurements = {}
        check_results = {}

        if type(historic_descriptor) == HistoricMeasurementsDescriptor:
            measurements = self._get_historic_measurements(historic_descriptor)
        elif type(historic_descriptor) == HistoricCheckResultsDescriptor:
            check_results = self._get_historic_check_results(historic_descriptor)
        elif type(historic_descriptor) == HistoricChangeOverTimeDescriptor:
            measurements = self._get_historic_changes_over_time(historic_descriptor)
        else:
            self.logs.error(f"Invalid Historic Descriptor provided {historic_descriptor}")

        return {"measurements": measurements, "check_results": check_results}

    def is_samples_disabled(self) -> bool:
        return self.organization_configuration.get(self.ORG_CONFIG_KEY_DISABLE_COLLECTING_WH_DATA, True)

    def get_check_attributes_schema(self) -> list(dict):
        response_json_dict = self._execute_query(
            {"type": "sodaCoreAvailableCheckAttributes"},
            query_name="get_check_attributes",
        )

        if response_json_dict and "results" in response_json_dict:
            return response_json_dict["results"]

        return []

    def get_check_identities(self, check_id: str) -> dict:
        payload = {"type": "sodaCoreCheckIdentities", "checkId": check_id}

        return self._execute_query(
            payload,
            query_name="get_check_identity",
        )

    def _get_historic_changes_over_time(self, hd: HistoricChangeOverTimeDescriptor):
        query = {
            "type": "sodaCoreHistoricMeasurements",
            "filter": {
                "type": "and",
                "andExpressions": [
                    {
                        "type": "equals",
                        "left": {"type": "columnValue", "columnName": "metric.identity"},
                        "right": {"type": "string", "value": hd.metric_identity},
                    }
                ],
            },
        }

        previous_time_start = None
        previous_time_end = None
        today = date.today()

        if hd.change_over_time_cfg.same_day_last_week:
            last_week = today - timedelta(days=7)
            previous_time_start = datetime(
                year=last_week.year, month=last_week.month, day=last_week.day, tzinfo=timezone.utc
            )
            previous_time_end = datetime(
                year=last_week.year,
                month=last_week.month,
                day=last_week.day,
                hour=23,
                minute=59,
                second=59,
                tzinfo=timezone.utc,
            )

        if previous_time_start and previous_time_end:
            query["filter"]["andExpressions"].append(
                {
                    "type": "greaterThanOrEqual",
                    "left": {"type": "columnValue", "columnName": "measurement.dataTime"},
                    "right": {"type": "time", "scanTime": False, "time": previous_time_start.isoformat()},
                }
            )
            query["filter"]["andExpressions"].append(
                {
                    "type": "lessThanOrEqual",
                    "left": {"type": "columnValue", "columnName": "measurement.dataTime"},
                    "right": {"type": "time", "scanTime": False, "time": previous_time_end.isoformat()},
                }
            )

        return self._execute_query(
            query,
            query_name="get_hisoric_changes_over_time",
        )

    def _get_historic_measurements(self, hd: HistoricMeasurementsDescriptor):
        historic_measurements = self._execute_query(
            {
                "type": "sodaCoreHistoricMeasurements",
                "limit": hd.limit,
                "filter": {
                    "type": "and",
                    "andExpressions": [
                        {
                            "type": "equals",
                            "left": {"type": "columnValue", "columnName": "metric.identity"},
                            "right": {"type": "string", "value": hd.metric_identity},
                        }
                    ],
                },
            },
            query_name="get_hisotric_check_results",
        )
        # Filter out historic_measurements not having 'value' key
        historic_measurements["results"] = [
            measurement for measurement in historic_measurements["results"] if "value" in measurement
        ]
        return historic_measurements

    def _get_historic_check_results(self, hd: HistoricCheckResultsDescriptor):
        return self._execute_query(
            {
                "type": "sodaCoreHistoricCheckResults",
                "limit": hd.limit,
                "filter": {
                    "type": "and",
                    "andExpressions": [
                        {
                            "type": "equals",
                            "left": {"type": "columnValue", "columnName": "check.identity"},
                            "right": {"type": "string", "value": hd.check_identity},
                        }
                    ],
                },
            },
            query_name="get_hisotric_check_results",
        )

    @property
    def organization_configuration(self) -> dict:
        if isinstance(self._organization_configuration, dict):
            return self._organization_configuration

        response_json_dict = self._execute_query(
            {"type": "sodaCoreCloudConfiguration"},
            query_name="get_organization_configuration",
        )
        self._organization_configuration = response_json_dict if isinstance(response_json_dict, dict) else {}

        return self._organization_configuration

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
            self.logs.debug(f"Soda Cloud {request_type} {request_log_name} body JSON: {log_body_text}")
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
                    f"Soda Cloud error for {request_type} {request_log_name} | status_code:{response.status_code} | "
                    f"X-Soda-Trace-Id:{trace_id} | response_text:{response.text}"
                )
            else:
                self.logs.debug(
                    f"Soda Cloud {request_type} {request_log_name} OK | X-Soda-Trace-Id:{trace_id}"
                )

            return response
        except Exception as e:
            self.logs.error(f"Error while executing Soda Cloud {request_type} {request_log_name}", exception=e)

    def _http_post(self, request_log_name: str = None, **kwargs) -> Response:
        return requests.post(**kwargs)

    def _execute_rest_get(
        self,
        relative_url_path: str,
        request_log_name: str,
        is_retry: bool = True
    ) -> Response:
        response: Response = self._http_get(
            url=f"{self.api_url}/v1/{relative_url_path}",
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
                f"Soda Cloud error for {request_log_name} | status_code:{response.status_code} | "
                f"X-Soda-Trace-Id:{trace_id} | response_text:{response.text}"
            )
        else:
            self.logs.debug(
                f"Soda Cloud {request_log_name} OK | X-Soda-Trace-Id:{trace_id}"
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
            return o.astimezone(timezone.utc).isoformat(timespec="seconds")
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


class HistoricDescriptor(ABC):
    pass


@dataclass(frozen=True)
class HistoricMeasurementsDescriptor(HistoricDescriptor):
    metric_identity: Optional[str]
    limit: Optional[int] = 100


@dataclass(frozen=True)
class HistoricCheckResultsDescriptor(HistoricDescriptor):
    check_identity: Optional[str]
    limit: Optional[int] = 100


@dataclass(frozen=True)
class HistoricChangeOverTimeDescriptor(HistoricDescriptor):
    metric_identity: Optional[str]
    change_over_time_cfg: ChangeOverTimeCfg()


class ChangeOverTimeCfg:
    def __init__(self):
        self.last_measurements: Optional[int] = None
        self.last_aggregation: Optional[str] = None
        self.same_day_last_week: bool = False
        self.same_day_last_month: bool = False
        self.percent: bool = False

    def to_jsonnable(self):
        jsonnable = {}
        if self.last_measurements:
            jsonnable["last_measurements"] = self.last_measurements
        if self.last_aggregation:
            jsonnable["last_aggregation"] = self.last_aggregation
        if self.same_day_last_week:
            jsonnable["same_day_last_week"] = self.same_day_last_week
        if self.same_day_last_month:
            jsonnable["same_day_last_month"] = self.same_day_last_month
        if self.percent:
            jsonnable["percent"] = self.percent
        return jsonnable
