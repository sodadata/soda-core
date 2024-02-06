from __future__ import annotations

import json
import os
from typing import Any, Dict

import requests
from soda.scan import Scan

from soda.scientific.anomaly_detection_v2.exceptions import (
    AuthenticationException,
    CheckIDNotFoundException,
)
from soda.scientific.anomaly_detection_v2.pydantic_models import (
    AnomalyHistoricalMeasurement,
)


class AnomalyDetectionData:
    def __init__(self, check_id: str) -> None:
        self.check_id = check_id
        self.check_results = self.get_check_results()
        self.measurements = self.create_measurements()

    def get_check_results(self) -> Dict[str, Any]:
        soda_cloud_token, api_url = self.get_soda_cloud_token_and_api_url()
        url = f"{api_url}/query?testResults"
        payload = json.dumps({"type": "testResults", "token": soda_cloud_token, "testId": self.check_id})
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 404:
            raise CheckIDNotFoundException(
                f"Check ID {self.check_id} does not point to an existing "
                "check or points to a check that you do not have access to. "
                "Please verify that the check URL is correct and "
                "that you have access to it."
            )
        check_results = response.json()

        # Sort check_results by scanTime
        check_results["results"] = sorted(check_results["results"], key=lambda k: k["scanTime"])
        return check_results

    @staticmethod
    def get_soda_cloud_token_and_api_url() -> tuple[str, str]:
        config_file_path = os.getenv("SODA_CONFIG_FILE_PATH")
        scan = Scan()
        scan.add_configuration_yaml_file(file_path=config_file_path)
        soda_cloud = scan._configuration.soda_cloud
        try:
            soda_cloud_token = soda_cloud._get_token()
        except AttributeError:
            raise AuthenticationException(
                f"Soda Cloud token not found. Please check your {config_file_path}"
                " file and make sure you have a valid api_key_id and api_key_secret."
            )
        api_url = soda_cloud.api_url
        return soda_cloud_token, api_url

    def create_measurements(self) -> Dict[str, Any]:
        measurements = {
            "results": [
                AnomalyHistoricalMeasurement(
                    id=check_result.get("measurementId", "dummy_id"),
                    identity="dummy_identity",
                    value=check_result["value"],
                    dataTime=check_result["scanTime"],
                ).model_dump()
                for check_result in self.check_results["results"]
            ]
        }
        return measurements
