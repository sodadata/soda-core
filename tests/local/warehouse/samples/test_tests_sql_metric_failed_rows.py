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
from sodasql.scan.metric import Metric
from tests.common.sql_test_case import SqlTestCase


class TestTestsSqlMetric(SqlTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.use_mock_soda_server_client()

        self.sql_recreate_table(
            [f"country {self.dialect.data_type_varchar_255}",
             f"size {self.dialect.data_type_integer}"],
            ["('one', 2)",
             "('two', 3)",
             "('one', 4) ",
             "('one', 5)",
             "('two', 6)"])
        self.qualified_table_name = self.warehouse.dialect.qualify_table_name(self.default_test_table_name)

    def test_sql_metric_failed_rows(self):

        scan_yml_dict = {
            'sql_metrics': [{
                'type': 'failed_rows',
                'name': 'large_ones',
                'sql': (
                    f"SELECT * \n"
                    f"FROM {self.qualified_table_name} \n"
                    f"WHERE country = 'one' and size > 2"
                )
            }]
        }

        scan_result = self.scan(scan_yml_dict=scan_yml_dict)
        self.assertTrue(scan_result.has_test_failures())

