from __future__ import annotations

import json
import os
import re
import tempfile
from abc import ABC
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone, time
from decimal import Decimal
from enum import Enum
from tempfile import TemporaryFile
from typing import Optional

import requests
from requests import Response

from soda_core.common.logs import Logs, Log
from soda_core.common.version import SODA_CORE_VERSION
from soda_core.common.yaml import YamlFileContent, YamlObject
from soda_core.contracts.contract_verification import ContractVerificationResult, ContractResult, \
    CheckResult


class SodaCloud:

    # Constants
    ORG_CONFIG_KEY_DISABLE_COLLECTING_WH_DATA = "disableCollectingWarehouseData"
    CSV_TEXT_MAX_LENGTH = 1500

    def from_file(self, soda_cloud_file_content: YamlFileContent):

        logs = soda_cloud_file_content.logs

        soda_cloud_configuration_yaml_object: YamlObject = (
            soda_cloud_file_content.get_yaml_object()
            if soda_cloud_file_content.has_yaml_object() else {}
        )

        def get_configuration(key: str, default_value: str | None = None, is_required: bool = True) -> str | None:
            """
            Looks for the key in the configuration_dict, if it exists
            If not, in the corresponding environment variable
            If not applies the default value
            """
            environment_variable_name: str = f"SODA_CLOUD_{key.upper()}"
            default_value = os.environ.get(environment_variable_name, default_value)
            value = soda_cloud_configuration_yaml_object.read_string_opt(key=key, default_value=default_value)
            if is_required and not isinstance(value, str):
                logs.error(f"Soda Cloud configuration '{key}' not provided as configuration nor environment variable")
            return value

        return SodaCloud(
            host=get_configuration(key="host", default_value="cloud.soda.io"),
            api_key_id=get_configuration(key="api_key_id"),
            api_key_secret=get_configuration(key="api_key_secret"),
            token=get_configuration(key="token", is_required=False),
            port=get_configuration(key="port", is_required=False),
            scheme=get_configuration(key="scheme", is_required=False),
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
        scheme: str = "https",
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

    def send_contract_result(self, contract_result: ContractResult):
        contract_result = self.build_contract_result_json(contract_result)
        contract_result["type"] = "sodaCoreInsertScanResults"
        self._execute_command(contract_result, command_name="send_scan_results")

    def build_contract_result_json(self, contract_result: ContractResult) -> dict:
        check_result_cloud_json_dicts = [
            self.build_check_result_cloud_dict(check_result)
            for check_result in contract_result.check_results
            # TODO ask m1no if this should be ported
            # if check.check_type == CheckType.CLOUD
            # and (check.outcome is not None or check.force_send_results_to_cloud is True)
            # and check.archetype is None
        ]

        log_cloud_json_dicts = [
            self.build_log_cloud_json_dict(log)
            for log in contract_result.logs.logs
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
            }
        )

    def build_check_result_cloud_dict(self, check_result: CheckResult) -> dict:
        return {
            "identities": {
                "vc1": check_result.check_identity
            }
        }
        #     // remain as-is, here is an example
        #     {
        #       "identities": {
        #                value needs to be unique within the organization
        #         "vc1": "2fefe263",
        #       },
        #
        #       UI label for the . (between 1 and 4000 chars)  Required, also if contract does not have name specified.
        #       "name": "Order_Records_Missing_in_the_Deliveries_Table_for_All_Markets",
        #
        #       "type": "generic",
        #       "definition": "checks for ORDERSCUBE:\n  - percentage_of_missing_orders > 100:\n      identity: 9aa2a4df\n      name: Order_Records_Missing_in_the_Deliveries_Table_for_All_Markets\n      percentage_of_missing_orders query: \"WITH orders AS (     SELECT *         \\\n        \\ FROM GLOBAL_BI.BUSINESS.orderscube AS o          JOIN product_analytics.base_grain.plan_status_hist\n        psh                     ON o.plan_id = psh.plan_nk           WHERE       \\\n        \\        expected_delivery_date > 9999999               AND              \\\n        \\ TO_DATE(TO_CHAR(expected_delivery_date), 'YYYYMMDD') = '2025-01-27'    \\\n        \\        AND box_type_level_1 = 'Mealboxes'            AND subscription_id\n        is not null            AND psh.plan_status_date_to = '9999-12-31'        \\\n        \\    AND psh.plan_status != 'Ineligible' ), deliveries AS (     SELECT * \\\n        \\    FROM product_analytics.landing.delivery_v1     WHERE delivery_state =\n        1         AND is_editable = FALSE         AND planned_delivery_date = '2025-01-27'\n        ) SELECT     CASE WHEN            ROUND(100 * COUNT_IF(deliveries.plan_id\n        IS NOT NULL) / NULLIF(COUNT(*),0),2) IS NULL THEN 0     ELSE           ROUND(100\n        * COUNT_IF(deliveries.plan_id IS NOT NULL) / NULLIF(COUNT(*),0),2)     END\n        AS percentage_of_quality FROM     orders     LEFT JOIN deliveries ON deliveries.plan_id\n        = orders.plan_id         AND hellofresh_delivery_week = hf_week; \"\n",
        #
        #       The list of valid resourceAttributes keys is stored on Soda Cloud
        #       "resourceAttributes": [],
        #
        #       "location": {
        #         All properties here are required
        #         "filePath": "sodacl_string.yml",
        #         "line": 44,
        #         "col": 3
        #       },

        #       "dataSource": "SnowflakeCon_GLOBAL_BI_BUSINESS",
        #       "table": "ORDERSCUBE",
        #       "datasetPrefix" : ""

        #       "column": null,

        #       let's see if we can skip metrics and see if it works
        #       "metrics": [
        #         "metric-contract://SnowflakeCon_GLOBAL_BI_BUSINESS/GLOBAL_BI/BUSINESS/ORDERSCUBE-SnowflakeCon_GLOBAL_BI_BUSINESS-percentage_of_missing_orders > 100-7253408e"
        #       ],
        #       "outcome": "fail",
        #       "diagnostics": {
        #         "blocks": [],
        #         "value": 99.97,
        #         "fail": {
        #           "lessThanOrEqual": 100.0
        #         }
        #       },
        #       "source": "soda-contract"
        #     }
        pass

    def build_log_cloud_dict(self, log: Log) -> dict:
        #       {
        #       "level": "info",
        #       "message": "All is good. No failures. No warnings. No errors.",
        #       "timestamp": "2025-02-07T13:38:36.400+00:00",
        #       "index": 20,
        #       "dataset": null,
        #       "column": null,
        #       "metric": null
        #     },
        pass

    @staticmethod
    def _serialize_file_upload_value(value):
        if value is None or isinstance(value, str) or isinstance(value, int) or isinstance(value, float):
            return value
        return str(value)

    def upload_sample(
            self, contract_verification_results: ContractVerificationResult, sample_rows: tuple[tuple],
            sample_file_name: str, samples_limit: int | None
    ) -> str:
        """
        :param sample_file_name: file name without extension
        :return: Soda Cloud file_id
        """

        # Keep the interface of this method backward compatible and allow for samples limit to be None, but do not continue with no limit in such case.
        if not samples_limit:
            samples_limit = 100

        try:
            scan_definition_name = contract_verification_results._scan_definition_name
            scan_data_timestamp = contract_verification_results._data_timestamp
            scan_folder_name = (
                f"{self._fileify(scan_definition_name)}"
                f'_{scan_data_timestamp.strftime("%Y%m%d%H%M%S")}'
                f'_{datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S")}'
            )

            with TemporaryFile() as temp_file:
                for row in sample_rows[0:samples_limit]:
                    row = [self._serialize_file_upload_value(v) for v in row]
                    rows_json_str = json.dumps(row)
                    rows_json_bytes = bytearray(rows_json_str, "utf-8")
                    temp_file.write(rows_json_bytes)
                    temp_file.write(b"\n")

                temp_file_size_in_bytes = temp_file.tell()
                temp_file.seek(0)

                file_path = f"{scan_folder_name}/" + f"{sample_file_name}.jsonl"

                file_id = self._upload_sample_http(scan_definition_name, file_path, temp_file, temp_file_size_in_bytes)

                return file_id

        except Exception as e:
            self.logs.error(f"Soda cloud error: Could not upload sample {sample_file_name}", exception=e)

    def _upload_sample_http(self, scan_definition_name: str, file_path, temp_file: TemporaryFile, file_size_in_bytes: int):
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
            self.logs.error(f"No fileId received in response: {upload_response_json}")
            return None
        else:
            return upload_response_json["fileId"]

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

    def _execute_query(self, query: dict, query_name: str):
        return self._execute_request("query", query, False, query_name)

    def _execute_command(self, command: dict, command_name: str):
        return self._execute_request("command", command, False, command_name)

    def _execute_request(self, request_type: str, request_body: dict, is_retry: bool, request_name: str):
        try:
            request_body["token"] = self._get_token()
            log_body_text: str = json.dumps(self.to_jsonnable(request_body), indent=2)
            self.logs.debug(f"HTTP post body JSON: {log_body_text}")
            response = self._http_post(
                url=f"{self.api_url}/{request_type}", headers=self.headers, json=request_body, request_name=request_name
            )
            response_json = response.json()
            if response.status_code == 401 and not is_retry:
                self.logs.debug("Authentication failed. Probably token expired. Re-authenticating...")
                self.token = None
                response_json = self._execute_request(request_type, request_body, True, request_name)
            elif response.status_code != 200:
                self.logs.error(
                    f"Error while executing Soda Cloud {request_type} response code: {response.status_code}"
                )
                self.logs.debug(response.text)
            return response_json
        except Exception as e:
            self.logs.error(f"Error while executing Soda Cloud {request_type}", exception=e)

    def _http_post(self, request_name: str = None, **kwargs) -> Response:
        response = requests.post(**kwargs)

        if request_name:
            trace_id = response.headers.get("X-Soda-Trace-Id")
            if trace_id:
                self.soda_cloud_trace_ids[request_name] = trace_id

        return response

    def _get_token(self) -> str:
        if not self.token:
            login_command = {"type": "login"}
            if self.api_key_id and self.api_key_secret:
                login_command["apiKeyId"] = self.api_key_id
                login_command["apiKeySecret"] = self.api_key_secret
            else:
                raise RuntimeError("No API KEY and/or SECRET provided ")

            login_response = self._http_post(
                url=f"{self.api_url}/command", headers=self.headers, json=login_command, request_name="get_token"
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
