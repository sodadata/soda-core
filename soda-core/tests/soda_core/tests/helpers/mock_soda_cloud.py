from __future__ import annotations

import json
from dataclasses import dataclass
from io import BytesIO
from tempfile import TemporaryFile

from requests import Response, Request

from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud


class MockResponse(Response):

    def __init__(
        self,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        json_dict: dict | None = None
    ):
        super().__init__()
        self.status_code = status_code
        if isinstance(headers, dict):
            self.headers.update(headers)
        if isinstance(json_dict, dict):
            rows_json_str = json.dumps(json_dict)
            rows_json_bytes = bytearray(rows_json_str, "utf-8")
            self.raw = BytesIO(rows_json_bytes)


@dataclass
class MockRequest:
    request_name: str = None,
    url: str | None = None,
    headers: dict[str, str] = None,
    json: dict | None = None,
    data: TemporaryFile | None = None


class MockSodaCloud(SodaCloud):

    def __init__(self):
        super().__init__(
            host="test",
            api_key_id="iiiiiiiiiiii",
            api_key_secret="sssssssssss",
            token="ttttttttttt",
            port="5555",
            scheme="https",
            logs=Logs(),
        )
        self.requests: list[MockRequest] = []

    def _http_post(
        self,
        request_name: str = None,
        url: str | None = None,
        headers: dict[str, str] = None,
        json: dict | None = None,
        data: TemporaryFile | None = None
    ) -> Response:
        self.requests.append(MockRequest(
            request_name=request_name,
            url=url,
            headers=headers,
            json=json,
            data=data
        ))
        return MockResponse(
            status_code=200,
            headers={},
            json_dict={}
        )
