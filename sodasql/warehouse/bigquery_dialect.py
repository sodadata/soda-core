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

from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.oauth2.service_account import Credentials

from sodasql.scan.parse_logs import ParseLogs
from sodasql.warehouse.dialect import Dialect


class BigQueryDialect(Dialect):

    def __init__(self, warehouse_configuration: dict, parse_logs: ParseLogs):
        super().__init__()
        self.account_info = json.loads(warehouse_configuration.get('account_info'))
        self.database = warehouse_configuration.get('database')
        self.schema = warehouse_configuration.get('schema')

    def create_connection(self):
        credentials = Credentials.from_service_account_info(self.account_info)
        project_id = self.account_info['project_id']
        client = bigquery.Client(project=project_id, credentials=credentials)
        return dbapi.Connection(client)
