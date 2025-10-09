from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from enum import Enum
from io import BytesIO
from tempfile import TemporaryFile
from typing import Optional

from requests import Response
from soda_core.common.soda_cloud import SodaCloud


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

        # This needs to be read during testing but it's not accecssible from the main code path
        # As a workaround, I'm stashing it in the mock soda cloud object when it's sent to soda cloud
        self.failed_rows_diagnostics: Optional[list[dict]] = None

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
        self.requests.append(MockRequest(url=url, headers=headers, json=json, data=data))
        if self._is_send_failed_rows_diagnostics_request(json):
            self.failed_rows_diagnostics = json["diagnostics"]
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, MockResponse):
                if method != response.method:
                    raise AssertionError("Wrong response method")
                if self._is_send_scan_results_request(json):
                    self._resolve_check_identities(json, response)
                if self._is_send_failed_rows_diagnostics_request(json):
                    self.failed_rows_diagnostics = json["diagnostics"]
                logging.debug(f"MockSodaCloud responds to {method} {url} with provided response")
                return response
        logging.debug(f"MockSodaCloud responds to {method} {url} with default empty 200 OK response")
        return MockResponse(status_code=200, headers={}, json_object={})

    def _is_send_scan_results_request(self, request_json: Optional[dict]) -> bool:
        return (
            isinstance(request_json, dict)
            and "type" in request_json
            and request_json["type"] == "sodaCoreInsertScanResults"
        )

    def _is_send_failed_rows_diagnostics_request(self, request_json: Optional[dict]) -> bool:
        return (
            isinstance(request_json, dict)
            and "type" in request_json
            and request_json["type"] == "sodaCoreAddFailedRowsDiagnostics"
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
