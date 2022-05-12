from __future__ import annotations

import json
import logging
import re
import tempfile
from datetime import datetime
from typing import Tuple

import requests
from requests import Response
from soda.__version__ import SODA_CORE_VERSION
from soda.common.json_helper import JsonHelper
from soda.common.logs import Logs
from soda.soda_cloud.historic_descriptor import (
    HistoricChangeOverTimeDescriptor,
    HistoricCheckResultsDescriptor,
    HistoricDescriptor,
    HistoricMeasurementsDescriptor,
)

logger = logging.getLogger(__name__)


class SodaCloud:
    def __init__(
        self, host: str, api_key_id: str, api_key_secret: str, token: str | None, port: str | None, logs: Logs
    ):
        self.host = host
        self.port = f":{port}" if port else ""
        self.api_url = f"https://{self.host}{self.port}/api"
        self.api_key_id = api_key_id
        self.api_key_secret = api_key_secret
        self.token: str | None = token
        self.headers = {"User-Agent": f"SodaCore/{SODA_CORE_VERSION}"}
        self.logs = logs

    @staticmethod
    def build_scan_results(scan) -> dict:
        return JsonHelper.to_jsonnable(
            {
                "definitionName": scan._scan_definition_name,
                "dataTimestamp": scan._data_timestamp,
                "scanStartTimestamp": scan._scan_start_timestamp,
                "scanEndTimestamp": scan._scan_end_timestamp,
                "hasErrors": scan.has_error_logs(),
                "hasWarnings": scan.has_check_warns(),
                "hasFailures": scan.has_check_fails(),
                "metrics": [metric.get_cloud_dict() for metric in scan._metrics],
                "checks": [check.get_cloud_dict() for check in scan._checks if not check.skipped],
            }
        )

    @staticmethod
    def _serialize_file_upload_value(value):
        if value is None or isinstance(value, str) or isinstance(value, int) or isinstance(value, float):
            return value
        return str(value)

    def upload_sample(self, scan: Scan, sample_rows: tuple[tuple], sample_file_name: str) -> str:
        """
        :param sample_file_name: file name without extension
        :return: Soda Cloud file_id
        """

        try:
            scan_definition_name = scan._scan_definition_name
            scan_data_timestamp = scan._data_timestamp
            scan_folder_name = (
                f"{self._fileify(scan_definition_name)}"
                f'_{scan_data_timestamp.strftime("%Y%m%d%H%M%S")}'
                f'_{datetime.now().strftime("%Y%m%d%H%M%S")}'
            )

            with tempfile.TemporaryFile() as temp_file:
                for row in sample_rows:
                    row = [self._serialize_file_upload_value(v) for v in row]
                    temp_file.write(bytearray(json.dumps(row), "utf-8"))
                    temp_file.write(b"\n")

                temp_file_size_in_bytes = temp_file.tell()
                temp_file.seek(0)

                file_path = f"{scan_folder_name}/" + f"{sample_file_name}.jsonl"

                file_id = self._upload_sample_http(scan_definition_name, file_path, temp_file, temp_file_size_in_bytes)

                return file_id

        except Exception as e:
            self.logs.error(f"Soda cloud error: Could not upload sample {sample_file_name}", exception=e)

    def _fileify(self, name: str):
        return re.sub(r"[^A-Za-z0-9_]+", "_", name).lower()

    def _upload_sample_http(self, scan_definition_name: str, file_path, temp_file, file_size_in_bytes: int):
        headers = {
            "Authorization": self._get_token(),
            "Content-Type": "application/octet-stream",
            "Is-V3": "true",
            "File-Path": file_path,
        }

        if file_size_in_bytes == 0:
            # because of https://github.com/psf/requests/issues/4215 we can't send content size
            # when the size is 0 since requests blocks then on I/O indefinitely
            self.logs.warning("Empty file upload detected, not sending Content-Length header")
        else:
            headers["Content-Length"] = str(file_size_in_bytes)

        upload_response = self._http_post(url=f"{self.api_url}/scan/upload", headers=headers, data=temp_file)
        upload_response_json = upload_response.json()

        if "fileId" not in upload_response_json:
            logger.error(f"No fileId received in response: {upload_response_json}")
        return upload_response_json["fileId"]

    def insert_scan_results(self, scan_results):
        scan_results["type"] = "sodaCoreInsertScanResults"
        return self._execute_command(scan_results)

    def get_historic_data(self, historic_descriptor: HistoricDescriptor):
        measurements = {}
        check_results = {}

        if type(historic_descriptor) == HistoricMeasurementsDescriptor:
            measurements = self._get_historic_measurements(historic_descriptor)
        elif type(historic_descriptor) == HistoricCheckResultsDescriptor:
            check_results = self._get_hisotric_check_results(historic_descriptor)
        elif type(historic_descriptor) == HistoricChangeOverTimeDescriptor:
            measurements = self._get_hisoric_changes_over_time(historic_descriptor)
        else:
            logger.error(f"Invalid Historic Descriptor provided {historic_descriptor}")

        return {"measurements": measurements, "check_results": check_results}

    def _get_hisoric_changes_over_time(self, hd: HistoricChangeOverTimeDescriptor):
        return self._execute_query(
            {
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
        )

    def _get_historic_measurements(self, hd: HistoricMeasurementsDescriptor):
        return self._execute_query(
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
            }
        )

    def _get_hisotric_check_results(self, hd: HistoricCheckResultsDescriptor):
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
            }
        )

    def _execute_query(self, command: dict):
        return self._execute_request("query", command, False)

    def _execute_command(self, command: dict):
        return self._execute_request("command", command, False)

    def _execute_request(self, request_type: str, request_body: dict, is_retry: bool):
        try:
            request_body["token"] = self._get_token()
            response = self._http_post(url=f"{self.api_url}/{request_type}", headers=self.headers, json=request_body)
            response_json = response.json()
            if response.status_code == 401 and not is_retry:
                logger.debug(f"Authentication failed. Probably token expired. Re-authenticating...")
                self.token = None
                response_json = self._execute_request(request_type, request_body, True)
            elif response.status_code != 200:
                self.logs.error(
                    f"Error while executing Soda Cloud {request_type} response code: {response.status_code}"
                )
            return response_json
        except Exception as e:
            self.logs.error(f"Error while executing Soda Cloud {request_type}", exception=e)

    def _get_token(self):
        if not self.token:
            login_command = {"type": "login"}
            if self.api_key_id and self.api_key_secret:
                logger.debug("> /api/command (login with API key credentials)")
                login_command["apiKeyId"] = self.api_key_id
                login_command["apiKeySecret"] = self.api_key_secret
            else:
                raise RuntimeError("No API KEY and/or SECRET provided ")

            login_response = self._http_post(url=f"{self.api_url}/command", headers=self.headers, json=login_command)
            if login_response.status_code != 200:
                raise AssertionError(
                    f"< {login_response.status_code} login failed. Server response code:{login_response.content}"
                )
            login_response_json = login_response.json()

            self.token = login_response_json.get("token")
            assert self.token, "No token in login response?!"
            logger.debug("< 200 (login ok, token received)")
        return self.token

    def _http_post(self, **kwargs) -> Response:
        return requests.post(kwargs)
