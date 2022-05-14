from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

from requests import Response

from soda.common.json_helper import JsonHelper
from soda.soda_cloud.historic_descriptor import (
    HistoricChangeOverTimeDescriptor,
    HistoricDescriptor,
    HistoricMeasurementsDescriptor,
)
from soda.soda_cloud.soda_cloud import SodaCloud

logger = logging.getLogger(__name__)


class TimeGenerator:
    def __init__(
        self,
        timestamp: datetime = datetime.now(),
        timedelta: timedelta = timedelta(days=-1),
    ):
        self.timestamp = timestamp
        self.timedelta = timedelta

    def next(self):
        self.timestamp += self.timedelta
        return self.timestamp


@dataclass
class MockResponse:
    status_code: int = 200
    headers: dict = None
    _json: dict = None

    def json(self):
        return self._json


class MockSodaCloud(SodaCloud):
    def __init__(self, scan):
        super().__init__(
            host="test_host",
            api_key_id="test_api_key",
            api_key_secret="test_api_key_secret",
            token=None,
            port=None,
            logs=scan._logs,
        )
        self.historic_metric_values: list = []
        self.files = {}
        self.scan_results: List[dict] = []

    def create_soda_cloud(self):
        return self

    def mock_historic_values(self, metric_identity: str, metric_values: list, time_generator=TimeGenerator()):
        """
        To learn the metric_identity: fill in any string, check the error log and capture the metric_identity from there
        """
        historic_metric_values = [
            {"identity": metric_identity, "id": i, "value": v, "dataTime": time_generator.next()}
            for i, v in enumerate(metric_values)
        ]
        self.add_historic_metric_values(historic_metric_values)

    def add_historic_metric_values(self, historic_metric_values: list[dict[str, object]]):
        """
        Each historic metric value is a dict like this:
            {'data_time': time_generator.next(),
             'metric': metric,
             'value': v
            }
        """
        self.historic_metric_values.extend(historic_metric_values)

    def get_historic_data(self, historic_descriptor: HistoricDescriptor):
        return self.__get_historic_data(historic_descriptor)

    def __get_historic_data(self, historic_descriptor):
        measurements = {}
        check_results = {}

        if type(historic_descriptor) == HistoricChangeOverTimeDescriptor and historic_descriptor.change_over_time_cfg:
            change_over_time_aggregation = historic_descriptor.change_over_time_cfg.last_aggregation
            if change_over_time_aggregation in ["avg", "min", "max"]:
                historic_metric_values = self.__get_historic_metric_values(historic_descriptor.metric)

                max_historic_values = historic_descriptor.change_over_time_cfg.last_measurements

                if max_historic_values < len(historic_metric_values):
                    historic_metric_values = historic_metric_values[:max_historic_values]

                historic_values = [historic_metric_value["value"] for historic_metric_value in historic_metric_values]

                if change_over_time_aggregation == "min":
                    value = min(historic_values)
                elif change_over_time_aggregation == "max":
                    value = max(historic_values)
                elif change_over_time_aggregation == "avg":
                    value = sum(historic_values) / len(historic_values)

                return value

            elif change_over_time_aggregation is None:
                historic_metric_values = self.__get_historic_metric_values(historic_descriptor.metric)
                if len(historic_metric_values) > 0:
                    previous_metric_value = historic_metric_values[0]
                    return previous_metric_value

        elif type(historic_descriptor) == HistoricMeasurementsDescriptor:
            measurements = self.__get_historic_metric_values(historic_descriptor.metric_identity)

        return {"measurements": measurements, "check_results": check_results}

    def pop_scan_result(self) -> dict:
        return self.scan_results.pop()

    def __get_historic_metric_values(self, metric_identity):
        historic_metric_values = [
            historic_metric_value
            for historic_metric_value in self.historic_metric_values
            if historic_metric_value["identity"] == metric_identity
        ]

        if not historic_metric_values:
            raise AssertionError(f"No historic measurements for metric {metric_identity}")

        if len(historic_metric_values) > 0:
            historic_metric_values.sort(key=lambda m: m["dataTime"], reverse=True)

        return {"results": historic_metric_values}

    def find_file_content_by_file_id(self, file_id: str) -> str:
        file_dict: dict = self.files.get(file_id)
        if file_dict:
            return file_dict.get('content')

    def find_check_result(self, index: int):
        scan_result = self.scan_results[0]
        checks = scan_result['checks']
        return checks[index]

    def _http_post(self, **kwargs) -> Response:
        url = kwargs.get("url")
        if url.endswith("api/command"):
            return self._mock_server_command(**kwargs)
        elif url.endswith("api/query"):
            return self._mock_server_query(**kwargs)
        elif url.endswith("api/scan/upload"):
            return self._mock_server_upload(**kwargs)
        data = kwargs.get("data")
        if data:
            kwargs["data"] = data.read().decode("utf-8")
        raise AssertionError(f"Unsupported request to mock soda cloud: {JsonHelper.to_json_pretty(kwargs)}")

    def _mock_server_command(self, url, headers, json):
        command_type = json.get("type")
        if command_type == "login":
            return self._mock_server_command_login(url, headers, json)
        elif command_type == "sodaCoreInsertScanResults":
            return self._mock_server_command_sodaCoreInsertScanResults(url, headers, json)
        raise AssertionError(f"Unsupported command type {command_type}")

    def _mock_server_command_login(self, url, headers, json):
        return MockResponse(status_code=200, _json={"token": "***"})

    def _mock_server_command_sodaCoreInsertScanResults(self, url, headers, json):
        self.scan_results.append(json)
        return MockResponse(status_code=200)

    def _mock_server_query(self, url, headers, json):
        raise AssertionError("TODO")

    def _mock_server_upload(self, url, headers, data):
        file_id = f"file-{len(self.files)}"
        self.files[file_id] = {
            "file_id": file_id,
            "file_path": headers.get("File-Path"),
            "content": data.read().decode("utf-8")
        }
        return MockResponse(status_code=200, _json={"fileId": file_id})
