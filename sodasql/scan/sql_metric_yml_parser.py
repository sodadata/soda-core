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
from typing import List

from sodasql.cli.file_system import FileSystemSingleton
from sodasql.scan.parser import Parser
from sodasql.scan.scan_yml_parser import ScanYmlParser
from sodasql.scan.sql_metric_yml import SqlMetricYml

KEY_SQL = 'sql'
KEY_TESTS = 'tests'
KEY_GROUP_FIELDS = 'group_fields'

VALID_SQL_METRIC_KEYS = [KEY_SQL, KEY_TESTS, KEY_GROUP_FIELDS]


class SqlMetricYmlParser(Parser):
    """
    Parses SQL metric yaml files
    """

    def __init__(self, sql_metric_dict: dict, sql_metric_path: str):
        super().__init__(description=sql_metric_path)

        if isinstance(sql_metric_dict, dict):
            self._push_context(object=sql_metric_dict, name=self.description)

            sql_metric_path_dir, sql_metric_file_name = FileSystemSingleton.INSTANCE.split(sql_metric_path)

            group_fields = self.get_list_optional(KEY_GROUP_FIELDS)
            sql = self.get_str_required(KEY_SQL)
            tests = ScanYmlParser.parse_tests(
                self,
                sql_metric_dict,
                KEY_TESTS,
                context_sql_metric_file_name=sql_metric_file_name)

            self.sql_metric: SqlMetricYml = SqlMetricYml(sql=sql,
                                                         file_name=sql_metric_file_name,
                                                         group_fields=group_fields,
                                                         tests=tests)

            self.check_invalid_keys(VALID_SQL_METRIC_KEYS)


        else:
            self.error('No SQL metric configuration provided')
