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
from sodasql.scan.sql_metric_yml_parser import SqlMetricYmlParser, KEY_SQL, KEY_TESTS, KEY_GROUP_FIELDS, \
    KEY_METRIC_NAMES
from tests.common.sql_test_case import SqlTestCase


class TestTestsSqlMetric(SqlTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.sql_recreate_table(
            [f"country {self.dialect.data_type_varchar_255}",
             f"size {self.dialect.data_type_integer}"],
            ["('one', 2)",
             "('two', 3)",
             "('one', 4) ",
             "('one', 5)",
             "('two', 6)"])
        self.qualified_table_name = self.warehouse.dialect.qualify_table_name(self.default_test_table_name)

    def test_sql_metric_default_simple(self):
        sql_metric_dicts = [
            {
                KEY_SQL: (
                    f"SELECT sum(size) as sum_ones \n"
                    f"FROM {self.qualified_table_name} \n"
                    f"WHERE country = 'one'"
                ),
                KEY_TESTS: [
                    'sum_ones < 20'
                ]
            }
        ]

        scan_result = self.scan(sql_metric_dicts=sql_metric_dicts)
        self.assertFalse(scan_result.has_failures())

        sql_metric_dicts[0][KEY_TESTS][0] = 'sum_ones < 10'

        scan_result = self.scan(sql_metric_dicts=sql_metric_dicts)
        self.assertTrue(scan_result.has_failures())

    def test_sql_metric_default_field_metric_name_mapping(self):
        scan_result = self.scan(sql_metric_dicts=[
            {
                KEY_METRIC_NAMES: ['sum_ones'],
                KEY_SQL: (
                    f"SELECT sum(size) \n"
                    f"FROM {self.qualified_table_name} \n"
                    f"WHERE country = 'one'"
                ),
                KEY_TESTS: ['sum_ones < 20']
            }
        ])
        self.assertFalse(scan_result.has_failures())

    def test_sql_metric_groups(self):
        sql_metric_dicts = [
            {
                KEY_SQL: (
                    f"SELECT country, sum(size) as total_size_per_country \n"
                    f"FROM {self.qualified_table_name} \n"
                    f"GROUP BY country"
                ),
                KEY_TESTS: {
                    'total_size_test': 'total_size_per_country < 20'
                },
                KEY_GROUP_FIELDS: ['country']
            }
        ]
        scan_result = self.scan(sql_metric_dicts=sql_metric_dicts)
        self.assertFalse(scan_result.has_failures())

        sql_metric_dicts[0][KEY_TESTS]['total_size_test'] = 'total_size_per_country < 10'

        scan_result = self.scan(sql_metric_dicts=sql_metric_dicts)
        self.assertTrue(scan_result.has_failures())
