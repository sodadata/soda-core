from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from enum import Enum
from io import BytesIO
from tempfile import TemporaryFile
from typing import Optional

from requests import Response
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.soda_cloud_dto import DatasetConfigurationDTO


class MockHttpMethod(Enum):
    POST = "post"
    GET = "get"


class MockResponse(Response):
    def __init__(
        self,
        method: MockHttpMethod = MockHttpMethod.POST,
        status_code: int = 200,
        headers: Optional[dict[str, str]] = None,
        json_object: Optional[dict] = None,
    ):
        super().__init__()
        self.method: MockHttpMethod = method
        self.status_code = status_code
        if isinstance(headers, dict):
            self.headers.update(headers)
        # The json_object is stored for overwriting later. See below in _resolve_check_identities
        self.json_object: Optional[dict] = None
        self.set_json_object(json_object)

    def set_json_object(self, json_object: Optional[dict]):
        self.json_object = json_object
        rows_json_str = json.dumps(json_object)
        rows_json_bytes = bytearray(rows_json_str, "utf-8")
        self.raw = BytesIO(rows_json_bytes)


@dataclass
class MockRequest:
    request_log_name: str = (None,)
    url: Optional[str] = (None,)
    headers: dict[str, str] = (None,)
    json: Optional[dict] = (None,)
    data: Optional[TemporaryFile] = None


class MockSodaCloud(SodaCloud):
    def __init__(self, responses: Optional[list[Optional[MockResponse]]] = None):
        super().__init__(
            host="mock.soda.io",
            api_key_id="mock-key-id",
            api_key_secret="mock-key-secret",
            token="mock-token",
            port="9999",
            scheme="https",
        )
        self.requests: list[MockRequest] = []
        self.responses: list[Optional[MockResponse]] = responses if isinstance(responses, list) else []
        self.dataset_configurations: dict[DatasetIdentifier, DatasetConfigurationDTO] = {}
        # Fixture-driven historic-data responses (OBSL-1007), keyed by identity.
        # Only consulted when at least one fixture is registered, so existing
        # tests using the `responses` list are unaffected.
        self.historic_measurements: dict[str, list[dict]] = {}
        self.historic_check_results: dict[str, list[dict]] = {}

    def _http_post(
        self,
        request_log_name: str = None,
        url: Optional[str] = None,
        headers: dict[str, str] = None,
        json: Optional[dict] = None,
        data: Optional[TemporaryFile] = None,
    ) -> Response:
        return self._http_handle(method=MockHttpMethod.POST, url=url, headers=headers, json=json, data=data)

    def _http_get(
        self,
        url: Optional[str] = None,
        headers: dict[str, str] = None,
        json: Optional[dict] = None,
        data: Optional[TemporaryFile] = None,
    ) -> Response:
        return self._http_handle(method=MockHttpMethod.GET, url=url, headers=headers, json=json, data=data)

    def _http_handle(
        self,
        method: MockHttpMethod,
        url: Optional[str],
        headers: dict[str, str],
        json: Optional[dict],
        data: Optional[TemporaryFile],
    ) -> Response:
        if data is not None and hasattr(data, "read"):
            data = data.read()
        self.requests.append(MockRequest(url=url, headers=headers, json=json, data=data))
        historic_response = self._try_handle_historic_request(json)
        if historic_response is not None:
            return historic_response
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, MockResponse):
                if method != response.method:
                    raise AssertionError("Wrong response method")
                if self._is_send_scan_results_request(json):
                    self._resolve_check_identities(json, response)
                logging.debug(f"MockSodaCloud responds to {method} {url} with provided response")
                return response
        logging.debug(f"MockSodaCloud responds to {method} {url} with default empty 200 OK response")
        return MockResponse(status_code=200, headers={}, json_object={})

    def add_historic_measurements(self, metric_identity: str, measurements: list[dict]):
        """Register fixture measurements returned for `sodaCoreHistoricMeasurements2`
        requests that include `metric_identity` in their `metricIdentities`."""
        self.historic_measurements.setdefault(metric_identity, []).extend(measurements)

    def add_historic_check_results(self, check_identity: str, check_results: list[dict]):
        """Register fixture check results returned for `sodaCoreHistoricCheckResults2`
        requests that include `check_identity` in their `checkIdentities`."""
        self.historic_check_results.setdefault(check_identity, []).extend(check_results)

    # Mirrors the Soda Cloud backend constraint: historic-data queries accept at
    # most 500 identities per request. Requests above that get a 400, so tests
    # prove SodaCloud batches (soda_cloud.HISTORIC_IDENTITIES_MAX_BATCH_SIZE).
    HISTORIC_IDENTITIES_MAX_BATCH_SIZE: int = 500

    def _try_handle_historic_request(self, request_json: Optional[dict]) -> Optional[MockResponse]:
        """Serve `sodaCoreHistoricMeasurements2`/`sodaCoreHistoricCheckResults2` from
        the registered fixtures. Returns None (fall through to the `responses` list)
        when the request is not a historic-data query or no fixtures are registered."""
        if not isinstance(request_json, dict):
            return None
        request_type = request_json.get("type")
        if request_type == "sodaCoreHistoricMeasurements2":
            fixtures, identities_key = self.historic_measurements, "metricIdentities"
        elif request_type == "sodaCoreHistoricCheckResults2":
            fixtures, identities_key = self.historic_check_results, "checkIdentities"
        else:
            return None
        if not fixtures:
            return None

        identities = request_json.get(identities_key) or []
        if len(identities) > self.HISTORIC_IDENTITIES_MAX_BATCH_SIZE:
            return MockResponse(
                status_code=400,
                json_object={
                    "code": "too_many_identities",
                    "message": f"Maximum {self.HISTORIC_IDENTITIES_MAX_BATCH_SIZE} identities per request",
                },
            )
        results: list[dict] = []
        for identity in identities:
            results.extend(fixtures.get(identity, []))
        return MockResponse(status_code=200, json_object={"results": results})

    def _is_send_scan_results_request(self, request_json: Optional[dict]) -> bool:
        return (
            isinstance(request_json, dict)
            and "type" in request_json
            and request_json["type"] == "sodaCoreInsertScanResults"
        )

    def _resolve_check_identities(self, request_json: dict, response: MockResponse):
        request_check_identities: list[str] = []
        request_check_results: Optional[list[dict]] = request_json.get("checks")
        if request_check_results:
            for request_check_result in request_check_results:
                check_identities: Optional[dict] = request_check_result.get("identities")
                if isinstance(check_identities, dict) and "vc1" in check_identities:
                    request_check_identities.append(check_identities["vc1"])

        new_json_object: dict = response.json_object.copy()
        response_checks: Optional[list[dict]] = new_json_object.get("checks")
        if isinstance(response_checks, list) and len(response_checks) == len(request_check_identities):
            for index in range(0, len(response_checks)):
                response_check: dict = response_checks[index]
                response_check["identities"] = [request_check_identities[index]]

        response.set_json_object(new_json_object)

    def set_dataset_configuration_response(
        self, dataset_identifier: "DatasetIdentifier", dataset_configuration_dto: "DatasetConfigurationDTO"
    ):
        self.dataset_configurations[str(dataset_identifier)] = dataset_configuration_dto

    def fetch_dataset_configuration(
        self, dataset_identifier: "DatasetIdentifier"
    ) -> Optional["DatasetConfigurationDTO"]:
        if str(dataset_identifier) in self.dataset_configurations:
            return self.dataset_configurations[str(dataset_identifier)]

        # Return an empty DatasetConfigurationDTO for testing purposes. This is sufficient for tests that do not depend on specific configuration values.
        # This overrides the mock response setup in tests, but it would be annoying to have to set up the mock response for every test just to return an empty configuration.
        return DatasetConfigurationDTO()
