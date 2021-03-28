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
from sodasql.scan.scan_yml_parser import KEY_SQL_METRICS, SQL_METRIC_KEY_TESTS, SQL_METRIC_KEY_SQL, \
    COLUMN_KEY_SQL_METRICS, SQL_METRIC_KEY_GROUP_FIELDS, COLUMN_KEY_METRICS
from tests.common.sql_test_case import SqlTestCase


class TestTestsSqlMetric(SqlTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.sql_recreate_table(
            table_name='xxx_view',
            columns=[f"r_id {self.dialect.data_type_varchar_255}",
                     f"s_id {self.dialect.data_type_varchar_255}",
                     f"p_id {self.dialect.data_type_varchar_255}",
                     f"week {self.dialect.data_type_integer}",
                     f"price {self.dialect.data_type_integer}"],
            rows=["('r1', 's1', 'p1', 1, 1)",
                  "('r1', 's1', 'p1', 1, 2)",
                  "('r1', 's1', 'p1', 1, 3) ",
                  "('r1', 's2', 'p2', 2, 10)",
                  "('r1', 's2', 'p2', 2, 20)",
                  "('r3', 's3', 'p3', 3, 50)"])
        self.qualified_table_name = self.warehouse.dialect.qualify_table_name(self.default_test_table_name)

    def test_sql_metric_groups_multi(self):
        scan_yml_dict = {
            'filter': "r_id = '{{ R_ID }}'",
            KEY_SQL_METRICS: [{
                SQL_METRIC_KEY_SQL: (
                    f"SELECT r_id, week, s_id, p_id, count(price) as prices_found \n"
                    f"FROM xxx_view \n"
                    f"WHERE r_id = '{{{{ R_ID }}}}' \n"
                    f"GROUP BY r_id, week, s_id, p_id \n"
                    f"ORDER BY r_id, week, s_id, p_id"
                ),
                SQL_METRIC_KEY_TESTS: [
                    "total_size_per_country < 20"
                ],
                SQL_METRIC_KEY_GROUP_FIELDS: ['r_id', 'week', 's_id', 'p_id'],
                SQL_METRIC_KEY_TESTS: [
                    "prices_found > 0"
                ]
            }]
        }

        scan_result = self.scan(scan_yml_dict=scan_yml_dict, variables={'R_ID': 'r1'})
        self.assertFalse(scan_result.has_test_failures())
