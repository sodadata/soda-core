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
        json_object: any = None,
    ):
        super().__init__()
        self.method: MockHttpMethod = method
        self.status_code = status_code
        if isinstance(headers, dict):
            self.headers.update(headers)
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
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, MockResponse):
                if method != response.method:
                    raise AssertionError("Wrong response method")
                logging.debug(f"MockSodaCloud responds to {method} {url} with provided response")
                return response
        logging.debug(f"MockSodaCloud responds to {method} {url} with default empty 200 OK response")
        return MockResponse(status_code=200, headers={}, json_object={})
