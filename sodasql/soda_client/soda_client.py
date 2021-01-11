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


class SodaClient:

    def __init__(self,
                 server: str,
                 port: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 api_key_id: Optional[str] = None,
                 api_key_secret: Optional[str] = None,
                 configure_logging: Optional[bool] = False):
        self.server_name: str = server
        port = f':{port}' if port else ''
        protocol = os.getenv('SODA_CLIENT_PROTOCOL', 'https')
        self.api_url: str = f'{protocol}://{self.server_name}{port}/api'
        self.username: Optional[str] = username
        self.password: Optional[str] = password
        self.api_key_id: Optional[str] = api_key_id
        self.api_key_secret: Optional[str] = api_key_secret
        self.token: Optional[str] = None
        if configure_logging:
            logging.getLogger('urllib3').setLevel(logging.WARNING)
            logging.basicConfig(level=logging.DEBUG,
                                # https://docs.python.org/3/library/logging.html#logrecord-attributes
                                format="%(asctime)s %(levelname)s | %(message)s",
                                handlers=[logging.StreamHandler(sys.stdout)])

    def send_measurements(self, scan_reference: dict, measurements_jsons: list):
        return self.execute_command({
            'type': 'addMeasurements',
            'scanReference': scan_reference,
            'measurements': measurements_jsons
        })

    def get_datasources(self):
        return self.execute_query({
            'type': 'datasources'
        })

    def create_datasource(self, name: str, configuration: dict):
        return self.execute_command({
            'type': 'createDatasource',
            'name': name,
            'configuration': configuration
        })

    def delete_datasource(self, datasource_id):
        return self.execute_command({
            'type': 'deleteDatasource',
            'datasourceId': datasource_id
        })

    def get_datasets(self):
        return self.execute_query({
            'type': 'datasets'
        })

    def create_dataset(self, datasource_id: str, configuration: dict, time_schedule: dict, name: Optional[str] = None):
        return self.execute_command({
            'type': 'createDataset',
            'name': name,
            'datasourceId': datasource_id,
            'configuration': configuration,
            'timeSchedule': time_schedule
        })

    def delete_dataset(self, dataset_id):
        return self.execute_command({
            'type': 'deleteDataset',
            'datasetId': dataset_id
        })

    def trigger_scan(self, dataset_id: str, timeslice: str):
        return self.execute_command({
            'type': 'triggerScan',
            'datasetId': dataset_id,
            'timeslice': timeslice
        })

    def get_columns(self, dataset_id: str):
        return self.execute_query({
            'type': 'columns',
            'datasetId': dataset_id
        })

    def update_settings(self, settings: dict):
        return self.execute_command({
            'type': 'updateSettings',
            'settings': settings
        })

    def get_settings(self, type: str):
        return self.execute_query({
            'type': f'{type}Settings'
        })

    def create_api_key(self, description: str):
        return self.execute_command({
            'type': 'createApiKey',
            'description': description
        })

    def update_column(self,
                      column_id: str,
                      name: Optional[str] = None,
                      missing_values: Optional[List[str]] = None,
                      validity: Optional[dict] = None):
        update_column_command = {
            'type': 'updateColumn',
            'columnId': column_id
        }
        if name:
            update_column_command['name'] = name
        if missing_values:
            update_column_command['missingValues'] = missing_values
        if validity:
            update_column_command['validity'] = validity
        return self.execute_command(update_column_command)

    def create_test(self,
                    name: str,
                    metric_type: str,
                    dataset_id: str,
                    column_name: Optional[str] = None,
                    warning: Optional[dict] = None,
                    critical: Optional[dict] = None,
                    filter: Optional[dict] = None,
                    group_by_column_names: Optional[List[str]] = None):
        return self.execute_command({
            'type': 'createTest',
            'name': name,
            'metricType': metric_type,
            'datasetId': dataset_id,
            'columnName': column_name,
            'warning': warning,
            'critical': critical,
            'filter': filter,
            'groupByColumnNames': group_by_column_names
        })

    def execute_command(self, command: dict):
        self.__ensure_session_token()
        return self.__execute_request('command', command, False)

    def execute_query(self, command: dict):
        self.__ensure_session_token()
        return self.__execute_request('query', command, False)

    def __execute_request(self, request_type: str, request_body: dict, is_retry: bool):
        logging.debug(f'> /api/{request_type} {json.dumps(request_body, indent=2)}')
        request_body['token'] = self.token
        response = requests.post(f'{self.api_url}/{request_type}', json=request_body)
        response_json = response.json()
        logging.debug(f'< {response.status_code} {json.dumps(response_json, indent=2)}')
        if response.status_code == 401 and not is_retry:
            logging.debug(f'Authentication failed. Probably token expired. Reauthenticating...')
            self.token = None
            self.__ensure_session_token()
            response_json = self.__execute_request(request_type, request_body, True)
        else:
            assert response.status_code == 200, f'Request failed with status {response.status_code}: {json.dumps(response_json, indent=2)}'
        return response_json

    def __ensure_session_token(self):
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
