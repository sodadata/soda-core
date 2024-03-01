from __future__ import annotations

import os
from typing import Any, Dict

from soda.cloud.historic_descriptor import HistoricCheckResultsDescriptor
from soda.cloud.soda_cloud import SodaCloud
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
        soda_cloud = self.get_soda_cloud()
        check_identities_response = soda_cloud.get_check_identities(check_id=self.check_id)
        check_identities = check_identities_response.get("identities")

        check_identity = None
        # First try to fetch v4 if v4 is not found then fetch v3
        if check_identities is not None:
            check_identity = check_identities.get("v4", check_identities.get("v3", None))

        if check_identity is None or check_identities is None:
            raise CheckIDNotFoundException(
                f"Check ID {self.check_id} does not point to an existing "
                "check or points to a check that you do not have access to. "
                "Please verify that the check URL is correct and "
                "that you have access to it."
            )

        historic_descriptor = HistoricCheckResultsDescriptor(check_identity=check_identity, limit=10000)
        check_results = soda_cloud._get_historic_check_results(hd=historic_descriptor)

        if check_results is None:
            raise CheckIDNotFoundException(
                f"Check ID {self.check_id} does not point to an existing "
                "check or points to a check that you do not have access to. "
                "Please verify that the check URL is correct and "
                "that you have access to it."
            )
        # Sort check_results by dataTime
        check_results["results"] = sorted(check_results["results"], key=lambda k: k["dataTime"])
        return check_results

    @staticmethod
    def get_soda_cloud() -> SodaCloud:
        config_file_path = os.getenv("SODA_CONFIG_FILE_PATH")
        scan = Scan()
        scan.add_configuration_yaml_file(file_path=config_file_path)
        soda_cloud = scan._configuration.soda_cloud
        try:
            soda_cloud._get_token()
        except AttributeError:
            raise AuthenticationException(
                f"Soda Cloud token not found. Please check your {config_file_path}"
                " file and make sure you have a valid api_key_id and api_key_secret."
            )
        return soda_cloud

    def create_measurements(self) -> Dict[str, Any]:
        measurements = {
            "results": [
                AnomalyHistoricalMeasurement(
                    id=check_result.get("measurementId", "dummy_id"),
                    identity="dummy_identity",
                    value=check_result["diagnostics"]["value"],
                    dataTime=check_result["dataTime"],
                ).model_dump()
                for check_result in self.check_results["results"]
            ]
        }
        return measurements
