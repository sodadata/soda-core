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
from json.decoder import JSONDecodeError

from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.oauth2.service_account import Credentials

from sodasql.scan.dialect import Dialect, BIGQUERY
from sodasql.scan.parser import Parser


class BigQueryDialect(Dialect):

    def __init__(self, parser: Parser):
        super().__init__()
        self.account_info_dict = self.__parse_json_credential('account_info_json', parser)
        self.dataset_name = parser.get_str_required('dataset')

    def default_connection_properties(self, params: dict):
        return {
            'type': BIGQUERY,
            'account_info': 'env_var(BIGQUERY_ACCOUNT_INFO)',
            'dataset': params.get('database', 'Eg your_bigquery_dataset')
        }

    def default_env_vars(self, params: dict):
        return {
            'BIGQUERY_ACCOUNT_INFO': '...'
        }

    def create_connection(self):
        credentials = Credentials.from_service_account_info(self.account_info_dict)
        project_id = self.account_info_dict['project_id']
        client = bigquery.Client(project=project_id, credentials=credentials)
        return dbapi.Connection(client)

    def sql_columns_metadata_query(self, scan_configuration):
        return (f"SELECT column_name, data_type, is_nullable "
                f'FROM `{self.dataset_name}.INFORMATION_SCHEMA.COLUMNS` '
                f"WHERE table_name = '{scan_configuration.table_name}';")

    @staticmethod
    def __parse_json_credential(credential_name, parser):
        try:
            return json.loads(parser.get_credential(credential_name))
        except JSONDecodeError as e:
            parser.error(f'Error parsing credential %s: %s', credential_name, e)
