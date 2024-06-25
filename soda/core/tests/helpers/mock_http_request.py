import json as json_module

from requests import Response
from dataclasses import dataclass

@dataclass
class MockResponse:
    status_code: int = 200
    headers: dict = None
    _json: dict = None

    def json(self):
        return self._json
    
    @property
    def text(self):
        if self._json:
            return self._json.get("text")
        return ''

class MockHttpRequest:
    @staticmethod
    def _create():
        return MockHttpRequest()

    def __init__(self):
        self.http_samples = {}

    def find_failed_rows_http_samples(self, check_name: str):
        http_samples = self.http_samples.get(check_name)
        if http_samples:
            return http_samples

    def assert_failed_rows_http_samples_present(self, check_name: str):
        assert self.find_failed_rows_http_samples(check_name) is not None

    def assert_failed_rows_http_samples_absent(self, check_name: str):
        assert self.find_failed_rows_http_samples(check_name) is None
    
    def mock_post(self, url, **kwargs) -> Response:
        if url.endswith("failed-rows.sampler.com"):
            return self._mock_http_sampler_request(url, **kwargs)
        raise AssertionError(f"Unsupported request to mock.")
    
    def _mock_http_sampler_request(self, url, json):
        json_obj = json_module.loads(json)
        check_name = json_obj.get("check_name")
        self.http_samples[check_name] = json_obj
        return MockResponse(status_code=200, _json={"text": "successfully uploaded"})