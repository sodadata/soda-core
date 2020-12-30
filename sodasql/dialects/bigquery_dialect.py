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
import os
from pathlib import Path

from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.oauth2.service_account import Credentials

from sodasql.scan.dialect import Dialect
from sodasql.scan.parse_logs import ParseLogs


class BigQueryDialect(Dialect):

    def __init__(self, warehouse_configuration: dict, parse_logs: ParseLogs):
        super().__init__()
        self.__load_account_info(warehouse_configuration)
        self.dataset_name = warehouse_configuration.get('dataset')

    def create_connection(self):
        credentials = Credentials.from_service_account_info(self.account_info)
        project_id = self.account_info['project_id']
        client = bigquery.Client(project=project_id, credentials=credentials)
        return dbapi.Connection(client)

    def sql_columns_metadata_query(self, scan_configuration):
        return (f"SELECT column_name, data_type, is_nullable "
                f'FROM `{self.dataset_name}.INFORMATION_SCHEMA.COLUMNS` '
                f"WHERE table_name = '{scan_configuration.table_name}';")

    def __load_account_info(self, warehouse_configuration):
        account_info_file = warehouse_configuration.get('account_info')
        if not os.path.isabs(account_info_file):
            account_info_file = os.path.join(Path.Home(), '.soda', account_info_file)
        with open(account_info_file) as f:
            self.account_info = json.load(f)
