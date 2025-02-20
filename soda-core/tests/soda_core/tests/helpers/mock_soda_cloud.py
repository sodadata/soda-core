from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from enum import Enum
from io import BytesIO
from tempfile import TemporaryFile

from requests import Response, Request

from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud


class MockHttpMethod(Enum):
    POST = "post"
    GET = "get"


class MockResponse(Response):

    def __init__(
        self,
        method: MockHttpMethod = MockHttpMethod.POST,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        json_dict: dict | None = None
    ):
        super().__init__()
        self.method: MockHttpMethod = method
        self.status_code = status_code
        if isinstance(headers, dict):
            self.headers.update(headers)
        if isinstance(json_dict, dict):
            rows_json_str = json.dumps(json_dict)
            rows_json_bytes = bytearray(rows_json_str, "utf-8")
            self.raw = BytesIO(rows_json_bytes)


@dataclass
class MockRequest:
    request_log_name: str = None,
    url: str | None = None,
    headers: dict[str, str] = None,
    json: dict | None = None,
    data: TemporaryFile | None = None


class MockSodaCloud(SodaCloud):

    def __init__(self, responses: list[MockResponse | None] | None = None):
        super().__init__(
            host="mock.soda.io",
            api_key_id="mock-key-id",
            api_key_secret="mock-key-secret",
            token="mock-token",
            port="9999",
            scheme="https",
            logs=Logs(),
        )
        self.requests: list[MockRequest] = []
        self.responses: list[MockResponse | None] = responses if isinstance(responses, list) else []

    def _http_post(
        self,
        request_log_name: str = None,
        url: str | None = None,
        headers: dict[str, str] = None,
        json: dict | None = None,
        data: TemporaryFile | None = None
    ) -> Response:
        return self._http_handle(
            method=MockHttpMethod.POST,
            request_log_name=request_log_name,
            url=url,
            headers=headers,
            json=json,
            data=data
        )

    def _http_get(
        self,
        request_log_name: str = None,
        url: str | None = None,
        headers: dict[str, str] = None,
        json: dict | None = None,
        data: TemporaryFile | None = None
    ) -> Response:
        return self._http_handle(
            method=MockHttpMethod.GET,
            request_log_name=request_log_name,
            url=url,
            headers=headers,
            json=json,
            data=data
        )

    def _http_handle(
        self,
        method: MockHttpMethod,
        request_log_name: str,
        url: str | None,
        headers: dict[str, str],
        json: dict | None,
        data: TemporaryFile | None
    ) -> Response:
        logging.debug(f"Request sent to MockSodaCloud: {request_log_name}")
        self.requests.append(MockRequest(
            request_log_name=request_log_name,
            url=url,
            headers=headers,
            json=json,
            data=data
        ))
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, MockResponse):
                if method != response.method:
                    raise AssertionError("Wrong response method")
                return response
        return MockResponse(
            status_code=200,
            headers={},
            json_dict={}
        )
