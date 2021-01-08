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
from typing import Optional, List

from sodasql.scan.parser import Parser
from sodasql.scan.scan_configuration_parser import ScanConfigurationParser
from sodasql.scan.sql_metric import SqlMetric
from sodasql.scan.sql_metric_configuration import SqlMetricConfiguration
from sodasql.scan.test import Test

KEY_NAME = 'name'
KEY_NAMES = 'names'
KEY_TYPE = 'type'
KEY_COLUMN = 'column'
KEY_SQL = 'sql'
KEY_TESTS = 'tests'

VALID_SQL_METRIC_KEYS = [KEY_NAME, KEY_TYPE, KEY_COLUMN, KEY_SQL, KEY_TESTS]


class SqlMetricConfigurationParser(Parser):
    """
    Parses SQL metric yaml files
    """

    def __init__(self, sql_metric_dict: dict, sql_metric_path: str):
        super().__init__(description=sql_metric_path)

        if isinstance(sql_metric_dict, dict):
            self._push_context(object=sql_metric_dict, name=self.description)

            type = self.get_str_required(KEY_TYPE)
            sql_metric_name = self.get_str_required(KEY_NAME)
            sql_metric_names = self.get_str_required(KEY_NAMES)
            column_name = self.get_str_optional(KEY_COLUMN)
            sql = self.get_str_required(KEY_SQL)
            tests = ScanConfigurationParser.parse_tests(
                self,
                sql_metric_dict,
                KEY_TESTS,
                'Sql metric tests',
                column_name,
                sql_metric_name)

            if type == SqlMetric.TYPE_FAILURES and not sql_metric_name:
                self.error(f'Type {SqlMetric.TYPE_FAILURES} requires a name')

            if type not in SqlMetric.ALL_SQL_METRIC_TYPES:
                self.error(f'Type {type} not valid.  Should be one of {str(SqlMetric.ALL_SQL_METRIC_TYPES)}')

            if sql_metric_name and sql_metric_names:
                self.error('Both name and names are specified')
            elif sql_metric_names is None and sql_metric_name:
                sql_metric_names = [sql_metric_name]

            self.sql_metric = SqlMetric(
                type, sql_metric_name, sql_metric_names, column_name, sql, tests)

            self.check_invalid_keys(VALID_SQL_METRIC_KEYS)

        else:
            self.error('No SQL metric configuration provided')
