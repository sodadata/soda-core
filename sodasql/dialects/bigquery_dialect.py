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

from google.api_core.exceptions import Forbidden, NotFound
from google.auth.exceptions import GoogleAuthError, TransportError
from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.oauth2.service_account import Credentials

from sodasql.scan.dialect import Dialect, BIGQUERY, KEY_WAREHOUSE_TYPE
from sodasql.scan.parser import Parser


class BigQueryDialect(Dialect):

    # TODO move these to test warehouse fixture
    data_type_varchar_255 = "STRING"
    data_type_integer = "INT64"
    data_type_decimal = "DECIMAL"
    data_type_bigint = "BIGNUMERIC"

    def __init__(self, parser: Parser):
        super().__init__(BIGQUERY)
        if parser:
            self.account_info_dict = self.__parse_account_info_json('account_info_json', parser)
            if not isinstance(self.account_info_dict, dict):
                self.account_info_dict = parser.get_file_json_dict_optional_env('account_info_file')
                if self.account_info_dict is None:
                    parser.error(f'Either account_info_json or account_info_file is required')
            self.dataset_name = parser.get_str_required('dataset')
        self.client = None

    @staticmethod
    def __parse_account_info_json(credential_name, parser):
        account_info_json_str = parser.get_str_optional_env(credential_name)
        if isinstance(account_info_json_str, str):
            try:
                return json.loads(account_info_json_str)
            except JSONDecodeError as e:
                parser.error(f'Error parsing credential {credential_name}: {e}', credential_name)
        return None

    def default_connection_properties(self, params: dict):
        return {
            KEY_WAREHOUSE_TYPE: BIGQUERY,
            'account_info_json': 'env_var(BIGQUERY_ACCOUNT_INFO_JSON)',
            'account_info_file': 'env_var(BIGQUERY_ACCOUNT_INFO_FILE)',
            'dataset': params.get('database', 'Eg your_bigquery_dataset')
        }

    def default_env_vars(self, params: dict):
        return {
            'BIGQUERY_ACCOUNT_INFO_JSON': '| ...remove this text and leave the | and paste '
                                          'the bigquery json below indented...',
            'BIGQUERY_ACCOUNT_INFO_FILE': 'Put path to the account info file. '
                                          'Full path or relative to warehouse.yml file'
        }

    def create_connection(self):
        try:
            credentials = Credentials.from_service_account_info(self.account_info_dict)
            project_id = self.account_info_dict['project_id']
            self.client = bigquery.Client(project=project_id, credentials=credentials)
            conn = dbapi.Connection(self.client)
            return conn
        except Exception as e:
            self.try_to_raise_soda_sql_exception(e)

    def sql_tables_metadata_query(self, limit: str = 10, filter: str = None):
        return (f"SELECT table_name \n"
                f"FROM `{self.dataset_name}.INFORMATION_SCHEMA.TABLES`;")

    def sql_columns_metadata_query(self, table_name: str):
        return (f"SELECT column_name, data_type, is_nullable "
                f'FROM `{self.dataset_name}.INFORMATION_SCHEMA.COLUMNS` '
                f"WHERE table_name = '{table_name}';")

    def is_text(self, column_type: str):
        return column_type.upper() in ['STRING']

    def is_number(self, column_type: str):
        return column_type.upper() in ['INT64', 'NUMERIC', 'DECIMAL', 'BIGNUMERIC', 'BIGDECIMAL', 'FLOAT64']

    def is_time(self, column_type: str):
        return column_type.upper() in ['DATE', 'DATETIME', 'TIME', 'TIMESTAMP']

    def qualify_table_name(self, table_name: str) -> str:
        return f'`{self.dataset_name}.{table_name}`'

    def qualify_writable_table_name(self, table_name: str) -> str:
        return self.qualify_table_name(table_name)

    def sql_expr_regexp_like(self, expr: str, pattern: str):
        return f"REGEXP_CONTAINS({expr}, r'{self.qualify_regex(pattern)}')"

    def qualify_table_name(self, table_name: str) -> str:
        return f'`{self.dataset_name}.{table_name}`'

    def qualify_writable_table_name(self, table_name: str) -> str:
        return self.qualify_table_name(table_name)

    def qualify_regex(self, regex):
        return regex.replace("''", "\\'")

    def qualify_string(self, value: str):
        return self.qualify_regex(value)

    def sql_expr_regexp_like(self, expr: str, pattern: str):
        return f"REGEXP_CONTAINS({expr}, r'{self.qualify_regex(pattern)}')"

    def sql_expr_cast_text_to_number(self, quoted_column_name, validity_format):
        if validity_format == 'number_whole':
            return f"CAST({quoted_column_name} AS {self.data_type_decimal})"
        not_number_pattern = self.qualify_regex(r"[^-\d\.\,]")
        comma_pattern = self.qualify_regex(r"\,")
        return f"CAST(REGEXP_REPLACE(REGEXP_REPLACE({quoted_column_name}, r'{not_number_pattern}', ''), " \
               f"r'{comma_pattern}', '.') AS {self.data_type_decimal})"

    def is_connection_error(self, exception):
        if exception is None:
            return False
        return isinstance(exception, NotFound) or \
                isinstance(exception, TransportError)

    def is_authentication_error(self, exception):
        if exception is None:
            return False
        return isinstance(exception, Forbidden) or \
               isinstance(exception, GoogleAuthError)
