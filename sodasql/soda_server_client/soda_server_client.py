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
from typing import Optional

import requests

from sodasql.scan.scan_yml import ScanYml
from sodasql.scan.scan_yml_column import ScanYmlColumn
from sodasql.version import SODA_SQL_VERSION


class SodaServerClient:

    def __init__(self,
                 host: str,
                 port: Optional[str] = None,
                 protocol: Optional[str] = 'https',
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 api_key_id: Optional[str] = None,
                 api_key_secret: Optional[str] = None,
                 token: Optional[str] = None):
        self.host: str = host
        colon_port = f':{port}' if port else ''
        self.api_url: str = f'{protocol}://{self.host}{colon_port}/api'
        self.username: Optional[str] = username
        self.password: Optional[str] = password
        self.api_key_id: Optional[str] = api_key_id
        self.api_key_secret: Optional[str] = api_key_secret
        self.token: Optional[str] = token

    def scan_start(self, warehouse, scan_yml: ScanYml, scan_time):
        soda_column_cfgs = {}
        if scan_yml.columns:
            for column_name in scan_yml.columns:
                scan_yml_column: ScanYmlColumn = scan_yml.columns[column_name]
                soda_column_cfg = {}
                if scan_yml_column.missing:
                    if scan_yml_column.missing.values:
                        soda_column_cfg['missingValues'] = scan_yml_column.missing.values
                    if scan_yml_column.missing.values:
                        soda_column_cfg['missingRegex'] = scan_yml_column.missing.regex
                    if scan_yml_column.missing.values:
                        soda_column_cfg['missingFormat'] = scan_yml_column.missing.format
                validity = scan_yml_column.validity
                if validity:
                    soda_column_cfg['validity'] = {
                        'namedFormat': validity.format,
                        'regexFormat': validity.regex,
                        'allowedValues': validity.values,
                        'minLength': validity.min_length,
                        'maxLength': validity.max_length,
                        'minValue': validity.min,
                        'maxValue': validity.max
                    }
                soda_column_cfgs[column_name] = soda_column_cfg

        return self.execute_command({
            'type': 'sodaSqlScanStart',
            'warehouseName': warehouse.name,
            'warehouseType': warehouse.dialect.type,
            'tableName': scan_yml.table_name,
            'scanTime': scan_time,
            'columns': soda_column_cfgs
        })

    def scan_ended(self, scan_reference, exception = None):
        if exception is None:
            self.execute_command({
                'type': 'sodaSqlScanEnd',
                'scanReference': scan_reference
            })
        else:
            self.execute_command({
                'type': 'sodaSqlScanEnd',
                'scanReference': scan_reference,
                'error': str(exception)
            })

    def scan_measurements(self, scan_reference: dict, measurement_jsons: list):
        return self.execute_command({
            'type': 'sodaSqlScanMeasurements',
            'scanReference': scan_reference,
            'measurements': measurement_jsons
        })

    def scan_test_results(self, scan_reference: dict, test_result_jsons: list):
        return self.execute_command({
            'type': 'sodaSqlScanTestResults',
            'scanReference': scan_reference,
            'testResults': test_result_jsons
        })

    def execute_command(self, command: dict):
        return self._execute_request('command', command, False)

    def execute_query(self, command: dict):
        return self._execute_request('query', command, False)

    def _execute_request(self, request_type: str, request_body: dict, is_retry: bool):
        logging.debug(f'> /api/{request_type} {json.dumps(request_body, indent=2)}')
        request_body['token'] = self.get_token()
        request_body['sodaSqlVersion'] = SODA_SQL_VERSION
        response = requests.post(f'{self.api_url}/{request_type}', json=request_body)
        response_json = response.json()
        logging.debug(f'< {response.status_code} {json.dumps(response_json, indent=2)}')
        if response.status_code == 401 and not is_retry:
            logging.debug(f'Authentication failed. Probably token expired. Reauthenticating...')
            self.token = None
            response_json = self._execute_request(request_type, request_body, True)
        else:
            assert response.status_code == 200, f'Request failed with status {response.status_code}: {json.dumps(response_json, indent=2)}'
        return response_json

    def get_token(self):
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
        return self.token
