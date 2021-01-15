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
from sodasql.scan.sql_metric_yml_parser import SqlMetricYmlParser, KEY_SQL, KEY_TESTS, KEY_GROUP_FIELDS
from tests.common.sql_test_case import SqlTestCase


class TestSqlMetricTests(SqlTestCase):

    def test_sql_metric_default(self):
        self.sql_create_table(
            self.default_test_table_name,
            [f"country {self.warehouse.dialect.string_column_type}",
             f"size {self.warehouse.dialect.integer_column_type}"],
            ["('one', 2)",
             "('two', 3)",
             "('one', 4) ",
             "('one', 5)",
             "('two', 6)"])

        dialect = self.warehouse.dialect
        qualified_table_name = dialect.qualify_table_name(self.default_test_table_name)

        scan_result = self.scan(sql_metric_dicts=[
            {
                KEY_SQL: (
                    f"SELECT sum(size) as sum_ones \n"
                    f"FROM {qualified_table_name} \n"
                    f"WHERE country = 'one'"
                ),
                KEY_TESTS: {
                    'sum_ones_20': 'sum_ones < 20'
                }
            }
        ])
        self.assertFalse(scan_result.has_failures())

        scan_result = self.scan(sql_metric_dicts=[
            {
                KEY_SQL: (
                    f"SELECT sum(size) as sum_ones \n"
                    f"FROM {qualified_table_name} \n"
                    f"WHERE country = 'one'"
                ),
                KEY_TESTS: {
                    'sum_ones_10': 'sum_ones < 10'
                }
            }
        ])
        self.assertTrue(scan_result.has_failures())

    def test_sql_metric_groups(self):
        self.sql_create_table(
            self.default_test_table_name,
            [f"country {self.warehouse.dialect.string_column_type}",
             f"size {self.warehouse.dialect.integer_column_type}"],
            ["('one', 2)",
             "('two', 3)",
             "('one', 4) ",
             "('one', 5)",
             "('two', 6)"])

        dialect = self.warehouse.dialect
        qualified_table_name = dialect.qualify_table_name(self.default_test_table_name)

        scan_result = self.scan(sql_metric_dicts=[
            {
                KEY_SQL: (
                    f"SELECT country, sum(size) as total_size_per_country \n"
                    f"FROM {qualified_table_name} \n"
                    f"GROUP BY country"
                ),
                KEY_TESTS: {
                    'total_size_20': 'total_size_per_country < 20'
                },
                KEY_GROUP_FIELDS: ['country']
            }
        ])
        self.assertFalse(scan_result.has_failures())

        scan_result = self.scan(sql_metric_dicts=[
            {
                KEY_SQL: (
                    f"SELECT country, sum(size) as total_size_per_country \n"
                    f"FROM {qualified_table_name} \n"
                    f"GROUP BY country"
                ),
                KEY_TESTS: {
                    'total_size_10': 'total_size_per_country < 10'
                },
                KEY_GROUP_FIELDS: ['country']
            }
        ])
        self.assertTrue(scan_result.has_failures())
