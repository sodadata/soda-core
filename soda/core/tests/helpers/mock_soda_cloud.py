from __future__ import annotations

import logging
from datetime import datetime, timedelta

from soda.scan import Scan
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
        return self.timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')


class MockSodaCloud(SodaCloud):
    def __init__(self):
        super().__init__(host="test_host", api_key_id="test_api_key", api_key_secret="test_api_key_secret")
        self.historic_metric_values: list = []
        self.scan_result: dict | None = None

    def create_soda_cloud(self):
        return self

    def send_scan_results(self, scan: Scan):
        if self.scan_result is not None:
            raise AssertionError("It is expected that send_scan_results is only called once is a single scan")
        self.scan_result = self.build_scan_results(scan)

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
        historic_data = {}

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
                historic_metric_values = self.__get_historic_metric_values(historic_descriptor.metric_identity)
                if len(historic_metric_values) > 0:
                    return {"measurements": historic_metric_values}

        elif type(historic_descriptor) == HistoricMeasurementsDescriptor:
            measurements = self.__get_historic_metric_values(historic_descriptor.metric_identity)

        return {"measurements": measurements, "check_results": check_results}

    def __get_historic_metric_values(self, metric_identity):
        if isinstance(metric_identity, str):
            historic_metric_values = [
                historic_metric_value
                for historic_metric_value in self.historic_metric_values
                if historic_metric_value["identity"] == metric_identity
            ]
        else:
            historic_metric_values = [
                historic_metric_value
                for historic_metric_value in self.historic_metric_values
                if historic_metric_value["identity"] == metric_identity.identity
            ]

        if not historic_metric_values:
            raise AssertionError(f"No historic measurements for metric {metric_identity}")

        if len(historic_metric_values) > 0:
            historic_metric_values.sort(key=lambda m: m["dataTime"], reverse=True)

        return {"results": historic_metric_values}


MOCK_SODA_CLOUD_INSTANCE = MockSodaCloud()
