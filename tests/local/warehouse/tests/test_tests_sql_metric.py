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
from sodasql.scan.scan_yml_parser import KEY_SQL_METRICS, SQL_METRIC_KEY_TESTS, SQL_METRIC_KEY_SQL, \
    COLUMN_KEY_SQL_METRICS, SQL_METRIC_KEY_GROUP_FIELDS, KEY_COLUMNS, COLUMN_KEY_METRICS
from sodasql.scan.sql_metric_yml_parser import KEY_SQL, KEY_TESTS, KEY_GROUP_FIELDS, \
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
        scan_yml_dict = {
            KEY_SQL_METRICS: [{
                SQL_METRIC_KEY_SQL: (
                    f"SELECT sum(size) as sum_ones \n"
                    f"FROM {self.qualified_table_name} \n"
                    f"WHERE country = 'one'"
                ),
                SQL_METRIC_KEY_TESTS: [
                    'sum_ones < 20'
                ]
            }]
        }

        scan_result = self.scan(scan_yml_dict=scan_yml_dict)
        self.assertFalse(scan_result.has_failures())

        scan_yml_dict[KEY_SQL_METRICS][0][SQL_METRIC_KEY_TESTS][0] = 'sum_ones < 10'

        scan_result = self.scan(scan_yml_dict=scan_yml_dict)
        self.assertTrue(scan_result.has_failures())

    def test_sql_metric_on_column(self):
        scan_yml_dict = {
            KEY_COLUMNS: {
                'size': {
                    COLUMN_KEY_METRICS: [
                        Metric.SUM
                    ],
                    COLUMN_KEY_SQL_METRICS: [{
                        SQL_METRIC_KEY_SQL: (
                            f"SELECT sum(size) as sum_ones \n"
                            f"FROM {self.qualified_table_name} \n"
                            f"WHERE country = 'one'"
                        ),
                        SQL_METRIC_KEY_TESTS: [
                            'sum - sum_ones == 9'
                        ]
                    }]
                }
            }
        }

        scan_result = self.scan(scan_yml_dict=scan_yml_dict)
        self.assertFalse(scan_result.has_failures())

        scan_yml_dict[KEY_COLUMNS]['size'][COLUMN_KEY_SQL_METRICS][0][SQL_METRIC_KEY_TESTS][0] = 'sum - sum_ones == 25'

        scan_result = self.scan(scan_yml_dict=scan_yml_dict)
        self.assertTrue(scan_result.has_failures())

    def test_sql_metric_default_field_metric_name_mapping(self):
        scan_yml_dict = {
            KEY_SQL_METRICS: [{
                KEY_METRIC_NAMES: ['sum_ones'],
                SQL_METRIC_KEY_SQL: (
                    f"SELECT sum(size) \n"
                    f"FROM {self.qualified_table_name} \n"
                    f"WHERE country = 'one'"
                ),
                SQL_METRIC_KEY_TESTS: [
                    'sum_ones < 20'
                ]
            }]
        }
        scan_result = self.scan(scan_yml_dict=scan_yml_dict)
        self.assertFalse(scan_result.has_failures())

    def test_sql_metric_groups(self):
        scan_yml_dict = {
            KEY_SQL_METRICS: [{
                SQL_METRIC_KEY_SQL: (
                    f"SELECT country, sum(size) as total_size_per_country \n"
                    f"FROM {self.qualified_table_name} \n"
                    f"GROUP BY country"
                ),
                SQL_METRIC_KEY_TESTS: [
                    'total_size_per_country < 20'
                ],
                SQL_METRIC_KEY_GROUP_FIELDS: ['country']
            }]
        }

        scan_result = self.scan(scan_yml_dict=scan_yml_dict)
        self.assertFalse(scan_result.has_failures())

        scan_yml_dict[KEY_SQL_METRICS][0][SQL_METRIC_KEY_TESTS][0] = 'total_size_per_country < 10'

        scan_result = self.scan(scan_yml_dict=scan_yml_dict)
        self.assertTrue(scan_result.has_failures())

