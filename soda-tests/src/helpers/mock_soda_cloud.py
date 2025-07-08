from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from io import BytesIO
from tempfile import TemporaryFile
from typing import Optional

from requests import Response

from soda_core.common.soda_cloud import SodaCloud


class MockHttpMethod(Enum):
    POST = "post"
    GET = "get"


@dataclass
class MockRequest:

    method: MockHttpMethod
    url: str
    headers: dict[str, str] = None
    json: Optional[dict] = None
    data: Optional[TemporaryFile] = None

    def is_url_upload(self) -> bool:
        return self.url.endswith("/upload")

    def is_command(self, command_type: str) -> bool:
        return (self.url.endswith("/command")
            and isinstance(self.json, dict)
            and "type" in self.json
            and self.json["type"] == command_type
        )

    def assert_json_subdict(self, expected: dict) -> None:
        self.__assert_json_expected_in(expected=expected, actual=self.json)

    @classmethod
    def __assert_json_expected_in(cls, expected, actual, path: str = "") -> None:
        """
        Recursively checks if `expected` is present within `actual` recursively.
        Handles dicts, lists, and basic data types.
        """
        if isinstance(expected, AssertStringInJson):
            expected.assert_string(actual, path)

        elif isinstance(expected, AssertFloatBetween):
            expected.assert_float(actual, path)

        elif isinstance(expected, dict):
            if not isinstance(actual, dict):
                raise AssertionError(f"Type mismatch at {path}: {actual} is not a dict")
            for key, expected_value in expected.items():
                cls.__assert_json_expected_in(
                    expected=expected_value,
                    actual=actual.get(key),
                    path=cls.__append_path(path=path, key=key)
                )

        elif isinstance(expected, list) or isinstance(expected, set) or isinstance(expected, tuple):
            if not (isinstance(actual, list) or isinstance(actual, set) or isinstance(actual, tuple)):
                raise AssertionError(f"Type mismatch at {path}: {actual} is not a dict")
            # Each item in subset must match some item in superset
            for index in range(0, len(expected)):
                expected_element: any = expected[index]
                actual_element: any = actual[index]
                cls.__assert_json_expected_in(expected_element, actual_element, cls.__append_path(path, str(index)))

        else:
            assert expected == actual, f"Value mismatch at {path}: Expected {expected}, but was {actual}"

    @classmethod
    def __append_path(cls, path: str, key: str) -> str:
        return key if path == "" else f"{path}.{key}"


class MockResponse(Response):
    def __init__(
        self,
        status_code: int = 200,
        headers: Optional[dict[str, str]] = None,
        json_object: Optional[dict] = None,
    ):
        super().__init__()
        self.status_code = status_code
        if isinstance(headers, dict):
            self.headers.update(headers)
        rows_json_str = json.dumps(json_object)
        rows_json_bytes = bytearray(rows_json_str, "utf-8")
        self.raw = BytesIO(rows_json_bytes)


class MockRequestHandler(ABC):

    @abstractmethod
    def handle(self, request: MockRequest, request_index: int) -> Optional[Response]:
        """
        returns a response if this MockRequestHandler is applicable, None if this handler is not for the given request and
        MockSodaCloud should try the next MockRequestHandler.
        """
        pass


class FileUploadRequestHandler(MockRequestHandler):

    def handle(self, request: MockRequest, request_index: int) -> Optional[Response]:
        if not request.is_url_upload():
            return None

        return MockResponse(
            json_object={"fileId": "the_file_id"}
        )


class SodaCoreInsertScanResultsHandler(MockRequestHandler):

    COMMAND_TYPE: str = "sodaCoreInsertScanResults"
    DEFAULT_SCAN_ID: str = "default-scan-id"

    def handle(self, request: MockRequest, request_index: int) -> Optional[Response]:
        if not request.is_command(self.COMMAND_TYPE):
            return None

        # Leverages the check identities from the request and creates check ids
        request_check_identities: list[str] = []
        request_check_results: Optional[list[dict]] = request.json.get("checks")
        if request_check_results:
            for request_check_result in request_check_results:
                check_identities: Optional[dict] = request_check_result.get("identities")
                if isinstance(check_identities, dict) and "vc1" in check_identities:
                    request_check_identities.append(check_identities["vc1"])

        response_json_dict: dict = {}
        response_json_dict["scanId"] = self.DEFAULT_SCAN_ID
        response_json_dict.setdefault("checks", [])
        response_checks: Optional[list[dict]] = response_json_dict.get("checks")
        if isinstance(response_checks, list):
            for index in range(0, len(request_check_identities)):
                if len(response_checks) <= index:
                    response_checks.append({})
                response_check: dict = response_checks[index]
                check_identity = request_check_identities[index]
                response_check["identities"] = [check_identity]
                check_id = f"checkid#for#{check_identity}################" # produces same length as UUI
                response_check["id"] = check_id

        return MockResponse(
            json_object=response_json_dict
        )


class SodaCoreAddFailedRowsDiagnosticsHandler(MockRequestHandler):

    COMMAND_TYPE: str = "sodaCoreAddFailedRowsDiagnostics"

    def handle(self, request: MockRequest, request_index: int) -> Optional[Response]:
        if not request.is_command(self.COMMAND_TYPE):
            return None
        return MockResponse()


class CatchAllMockRequestHandler(MockRequestHandler):

    def handle(self, request: MockRequest, request_index: int) -> Optional[Response]:
        return MockResponse()


class SequentialResponseRequestHandler(MockRequestHandler):

    def __init__(self, responses: list[MockResponse]):
        self.responses: list[MockResponse] = responses

    def handle(self, request: MockRequest, request_index: int) -> Optional[Response]:
        return self.responses[request_index]


class AssertStringInJson:
    def __init__(
        self,
        contains: Optional[str] = None,
        is_not_empty: bool = True
    ):
        self.contains: str = contains
        self.is_not_empty: bool = is_not_empty

    def assert_string(self, actual: any, path: str):
        assert isinstance(actual, str), f"Expected string at {path}, but was {actual}"
        if self.contains:
            assert self.contains in actual, f"Expected '{self.contains}' at {path}, but was {actual}"
        if self.is_not_empty:
            assert len(actual) > 0, f"Expected string at {path} to be non empty, but was empty"


class AssertFloatBetween:
    def __init__(
        self,
        min: float,
        max: float,
    ):
        self.min: float = min
        self.max: float = max

    def assert_float(self, actual: any, path: str):
        assert isinstance(actual, float), f"Expected string at {path}, but was {actual}"
        assert self.min <= actual <= self.max, f"Expected value between {self.min} and {self.max}, but was {actual}"


@dataclass
class MockRequestResponse:
    request: MockRequest
    response: Response


class MockSodaCloud(SodaCloud):

    DEFAULT_REQUEST_HANDLERS: list[MockRequestHandler] = [
        FileUploadRequestHandler(),
        SodaCoreInsertScanResultsHandler(),
        SodaCoreAddFailedRowsDiagnosticsHandler(),
        CatchAllMockRequestHandler(),
    ]

    def __init__(self, request_handlers: Optional[list[MockRequestHandler]] = None):
        super().__init__(
            host="mock.soda.io",
            api_key_id="mock-key-id",
            api_key_secret="mock-key-secret",
            token="mock-token",
            port="9999",
            scheme="https",
        )
        self._request_responses: list[MockRequestResponse] = []
        self._request_handlers: list[MockRequestHandler] = (
            self.DEFAULT_REQUEST_HANDLERS
            if request_handlers is None else request_handlers
        )

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
        request: MockRequest = MockRequest(method=method, url=url, headers=headers, json=json, data=data)
        for request_handler in self._request_handlers:
            request_index: int = len(self._request_responses)
            response: Optional[Response] = request_handler.handle(request, request_index)
            if response:
                self._request_responses.append(MockRequestResponse(request=request, response=response))
                return response
        raise NotImplementedError(f"No mock handler registered for request {request}.")

    def get_request(self, command_type: str, index: int = 0) -> MockRequest:
        return self.get_request_response(command_type=command_type, index=index).request

    def get_request_insert_scan_results(self, index: int = 0) -> MockRequest:
        return self.get_request_response(command_type="sodaCoreInsertScanResults", index=index).request

    def get_request_add_failed_rows_diagnostics(self, index: int = 0) -> MockRequest:
        return self.get_request_response(command_type="sodaCoreAddFailedRowsDiagnostics", index=index).request

    def get_request_response(self, command_type: str, index: int = 0) -> MockRequestResponse:
        command_request_responses: list[MockRequestResponse] = [
            request_response
            for request_response in self._request_responses
            if request_response.request.is_command(command_type)
        ]
        if len(command_request_responses) > index:
            return command_request_responses[index]
        else:
            raise AssertionError(f"No command request response found for command type {command_type}, index {index}")
