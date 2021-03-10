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

import boto3
import psycopg2
from botocore.exceptions import ClientError, ValidationError, ParamValidationError

from sodasql.dialects.postgres_dialect import PostgresDialect
from sodasql.scan.dialect import REDSHIFT, KEY_WAREHOUSE_TYPE
from sodasql.scan.dialect_parser import DialectParser
from sodasql.scan.parser import Parser


class RedshiftSpectrumDialect(PostgresDialect):
    def __init__(self, parser: Parser):
        super().__init__(parser, REDSHIFT)

    def sql_tables_metadata_query(self, limit: str = 10, filter: str = None):
        return (f"SELECT table_name \n"
                f"FROM SVV_EXTERNAL_TABLES \n"
                f"WHERE lower(schemaname)='{self.schema.lower()}'")

    def sql_columns_metadata_query(self, table_name: str) -> str:
        sql = (f"SELECT columnname, external_type, is_nullable \n"
               f"FROM SVV_EXTERNAL_COLUMNS \n"
               f"WHERE lower(tablename) = '{table_name}' \n"
               f"AND lower(schemaname)='{self.schema.lower()}'")
        return sql
