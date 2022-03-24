import logging
from typing import Optional

import requests
from soda.__version__ import SODA_CORE_VERSION

logger = logging.getLogger(__name__)


class SodaCloudClient:
    def __init__(
        self,
        api_key_id: str,
        api_key_secret: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        token: Optional[str] = None,
    ):
        self.host = host
        self.port = f":{port}" if port else ""
        self.api_url = f"https://{self.host}{self.port}/api"
        self.api_key_id = api_key_id
        self.api_key_secret = api_key_secret
        self.token: Optional[str] = token
        self.headers = {"User-Agent": f"SodaCore/{SODA_CORE_VERSION}"}

    def insert_scan_results(self, scan_results):
        scan_results["type"] = "sodaCoreInsertScanResults"
        return self._execute_command(scan_results)

    def _execute_command(self, command: dict):
        return self._execute_request("command", command, False)

    def _execute_request(self, request_type: str, request_body: dict, is_retry: bool):
        request_body["token"] = self._get_token()
        response = requests.post(f"{self.api_url}/{request_type}", json=request_body, headers=self.headers)
        response_json = response.json()
        if response.status_code == 401 and not is_retry:
            logger.debug(f"Authentication failed. Probably token expired. Re-authenticating...")
            self.token = None
            response_json = self._execute_request(request_type, request_body, True)
        elif response.status_code != 200:
            logger.debug("Unsupported feature")
        return response_json

    def _get_token(self):
        if not self.token:
            login_command = {"type": "login"}
            if self.api_key_id and self.api_key_secret:
                logger.debug("> /api/command (login with API key credentials)")
                login_command["apiKeyId"] = self.api_key_id
                login_command["apiKeySecret"] = self.api_key_secret
            else:
                raise RuntimeError("No API KEY and/or SECRET provided ")

            login_response = requests.post(f"{self.api_url}/command", json=login_command, headers=self.headers)

            if login_response.status_code != 200:
                raise AssertionError(f"< {login_response.status_code} Login failed: {login_response.content}")
            login_response_json = login_response.json()
            self.token = login_response_json.get("token")
            assert self.token, "No token in login response?!"
            logger.debug("< 200 (login ok, token received)")
        return self.token
