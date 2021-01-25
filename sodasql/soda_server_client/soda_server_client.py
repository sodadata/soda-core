#  Copyright 2020 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import json
import logging
import os
import sys
from typing import Optional, List

import requests

from sodasql.scan.measurement import Measurement


class SodaServerClient:

    def __init__(self,
                 host: str,
                 colon_port: Optional[str] = None,
                 protocol: Optional[str] = 'https',
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 api_key_id: Optional[str] = None,
                 api_key_secret: Optional[str] = None):
        self.host: str = host
        colon_port = f':{colon_port}' if colon_port else ''
        self.api_url: str = f'{protocol}://{self.host}{colon_port}/api'
        self.username: Optional[str] = username
        self.password: Optional[str] = password
        self.api_key_id: Optional[str] = api_key_id
        self.api_key_secret: Optional[str] = api_key_secret
        self.token: Optional[str] = None

    def send_measurements(self, scan_reference: dict, measurements_jsons: list):
        return self.execute_command({
            'type': 'addMeasurements',
            'scanReference': scan_reference,
            'measurements': measurements_jsons
        })

    def execute_command(self, command: dict):
        self._ensure_session_token()
        return self._execute_request('command', command, False)

    def execute_query(self, command: dict):
        self._ensure_session_token()
        return self._execute_request('query', command, False)

    def _execute_request(self, request_type: str, request_body: dict, is_retry: bool):
        logging.debug(f'> /api/{request_type} {json.dumps(request_body, indent=2)}')
        request_body['token'] = self.token
        response = requests.post(f'{self.api_url}/{request_type}', json=request_body)
        response_json = response.json()
        logging.debug(f'< {response.status_code} {json.dumps(response_json, indent=2)}')
        if response.status_code == 401 and not is_retry:
            logging.debug(f'Authentication failed. Probably token expired. Reauthenticating...')
            self.token = None
            self._ensure_session_token()
            response_json = self._execute_request(request_type, request_body, True)
        else:
            assert response.status_code == 200, f'Request failed with status {response.status_code}: {json.dumps(response_json, indent=2)}'
        return response_json

    def _ensure_session_token(self):
        if not self.token:
            login_command = {
                'type': 'login'
            }
            if self.api_key_id and self.api_key_secret:
                logging.debug('> /api/command (login with API key credentials)')
                login_command['apiKeyId'] = self.api_key_id
                login_command['apiKeySecret'] = self.api_key_secret
            elif self.username and self.password:
                logging.debug('> /api/command (login with username and password)')
                login_command['username'] = self.username
                login_command['password'] = self.password
            else:
                raise RuntimeError('No authentication in environment variables')

            login_response = requests.post(f'{self.api_url}/command', json=login_command)

            if login_response.status_code != 200:
                raise AssertionError(f'< {login_response.status_code} Login failed: {login_response.content}')
            login_response_json = login_response.json()
            self.token = login_response_json.get('token')
            assert self.token, 'No token in login response?!'
            logging.debug('< 200 (login ok, token received)')
