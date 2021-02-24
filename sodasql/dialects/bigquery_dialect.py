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
    data_type_varchar_255 = "STRING"
    data_type_integer = "INT64"
    data_type_decimal = "DECIMAL"
    data_type_bigint = "BIGNUMERIC"

    def __init__(self, parser: Parser):
        super().__init__(BIGQUERY)
        if parser:
            self.account_info_dict = self.__parse_json_credential('account_info_json', parser)
            self.dataset_name = parser.get_str_required('dataset')
        self.client = None

    def default_connection_properties(self, params: dict):
        return {
            KEY_WAREHOUSE_TYPE: BIGQUERY,
            'account_info': 'env_var(BIGQUERY_ACCOUNT_INFO)',
            'dataset': params.get('database', 'Eg your_bigquery_dataset')
        }

    def default_env_vars(self, params: dict):
        return {
            'BIGQUERY_ACCOUNT_INFO': '...'
        }

    def create_connection(self, *args, **kwargs):
        credentials = Credentials.from_service_account_info(self.account_info_dict)
        project_id = self.account_info_dict['project_id']
        self.client = bigquery.Client(project=project_id, credentials=credentials)
        return dbapi.Connection(self.client)

    def sql_tables_metadata_query(self, limit: str = 10, filter: str = None):
        return (f"SELECT table_name \n"
                f"FROM `{self.dataset_name}.INFORMATION_SCHEMA.TABLES`;")

    def sql_columns_metadata_query(self, table_name: str):
        return (f"SELECT column_name, data_type, is_nullable "
                f'FROM `{self.dataset_name}.INFORMATION_SCHEMA.COLUMNS` '
                f"WHERE table_name = '{table_name}';")

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

    @staticmethod
    def __parse_json_credential(credential_name, parser):
        try:
            return json.loads(parser.get_credential(credential_name))
        except JSONDecodeError as e:
            parser.error(f'Error parsing credential {credential_name}: {e}', credential_name)

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
