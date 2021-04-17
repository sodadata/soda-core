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
from sodasql.scan.scan_yml_parser import KEY_METRIC_GROUPS
from tests.common.sql_test_case import SqlTestCase
from decimal import *


class TestMetricGroups(SqlTestCase):

    def test_metric_group_duplicates(self):
        self.sql_recreate_table(
            [f"score {self.dialect.data_type_varchar_255}"],
            ["('1')",
             "('2')",
             "('2')",
             "('3')",
             "('3')",
             "('3')",
             "('3')",
             "('3')",
             "('4')",
             "('4')",
             "('5')",
             "(null)"])

        scan_result = self.scan({
            KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_DUPLICATES
            ]
        })

        self.assertEqual(scan_result.get(Metric.DISTINCT), 5)
        self.assertEqual(scan_result.get(Metric.DUPLICATE_COUNT), 3)
        self.assertEqual(scan_result.get(Metric.UNIQUENESS), 40)
        self.assertEqual(scan_result.get(Metric.UNIQUE_COUNT), 2)

    def test_metric_group_length(self):
        self.sql_recreate_table(
            [f"score {self.dialect.data_type_varchar_255}"],
            ["('1')",
             "('2')",
             "('2')",
             "('3')",
             "('3')",
             "('3')",
             "('3')",
             "('3')",
             "('4')",
             "('4')",
             "('5')",
             "(null)"])

        scan_result = self.scan({
            KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_LENGTH
            ]
        })
        self.assertEqual(scan_result.get(Metric.AVG_LENGTH), 1)
        self.assertEqual(scan_result.get(Metric.MAX_LENGTH), 1)
        self.assertEqual(scan_result.get(Metric.MIN_LENGTH), 1)

    def test_metric_group_missing(self):
        self.sql_recreate_table(
            [f"score {self.dialect.data_type_varchar_255}"],
            ["('1')",
             "('2')",
             "('2')",
             "('3')",
             "('3')",
             "('3')",
             "('3')",
             "('3')",
             "('4')",
             "('4')",
             "('5')",
             "(null)"])

        scan_result = self.scan({
            KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_MISSING
            ]
        })
        self.assertEqual(scan_result.get(Metric.MISSING_COUNT), 1)
        self.assertEqual(scan_result.get(Metric.MISSING_PERCENTAGE), 8.333333333333334)  # (1/12)%
        self.assertEqual(scan_result.get(Metric.VALUES_COUNT), 11)
        self.assertEqual(scan_result.get(Metric.VALUES_PERCENTAGE), 91.66666666666667)  # (11/12)%

    def test_metric_group_profiling(self):
        self.sql_recreate_table(
            [f"score {self.dialect.data_type_varchar_255}"],
            ["('1')",
             "('2')",
             "('2')",
             "('3')",
             "('3')",
             "('3')",
             "('3')",
             "('3')",
             "('4')",
             "('4')",
             "('5')",
             "(null)"])

        scan_result = self.scan({
            KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_PROFILING
            ]
        })
        self.assertCountEqual(scan_result.get(Metric.FREQUENT_VALUES),
                              [{'frequency': 5, 'value': '3'},
                               {'frequency': 2, 'value': '2'},
                               {'frequency': 2, 'value': '4'},
                               {'frequency': 1, 'value': '5'},
                               {'frequency': 1, 'value': '1'}]
                              )

        self.assertEqual(scan_result.get(Metric.MAXS), ['5', '4', '3', '2', '1'])
        self.assertEqual(scan_result.get(Metric.MINS), ['1', '2', '3', '4', '5'])

    def test_metric_group_statistics(self):
        self.sql_recreate_table(
            [f"score {self.dialect.data_type_integer}"],
            ["(1)",
             "(2)",
             "(2)",
             "(3)",
             "(3)",
             "(3)",
             "(3)",
             "(3)",
             "(4)",
             "(4)",
             "(5)",
             "(null)"])

        scan_result = self.scan({
            KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_STATISTICS
            ]
        })

        self.assertEqual(scan_result.get(Metric.AVG), 3)
        self.assertEqual(scan_result.get(Metric.SUM), 33)
        self.assertEqual(scan_result.get(Metric.MAX), 5)
        self.assertEqual(scan_result.get(Metric.MIN), 1)
        self.assertAlmostEqual(scan_result.get(Metric.STDDEV), Decimal(1.09544), 4)
        self.assertAlmostEqual(scan_result.get(Metric.VARIANCE), Decimal(1.2000), 4)

    def test_metric_group_validity(self):
        self.sql_recreate_table(
            [f"score {self.dialect.data_type_varchar_255}"],
            ["('1')",
             "('2')",
             "('2')",
             "('3')",
             "('3')",
             "('3')",
             "('3')",
             "('3')",
             "('4')",
             "('4')",
             "('5')",
             "(null)"])

        scan_result = self.scan({
            KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_VALIDITY
            ]
        })

        self.assertEqual(scan_result.get(Metric.INVALID_COUNT), 0)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE), 0.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT), 11)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE), 91.66666666666667)

    def test_metric_group_all_text(self):
        self.sql_recreate_table(
            [f"score {self.dialect.data_type_varchar_255}"],
            ["(1)",
             "(2)",
             "(2)",
             "(3)",
             "(3)",
             "(3)",
             "(3)",
             "(3)",
             "(4)",
             "(4)",
             "(5)",
             "(null)"])

        scan_result = self.scan({
            KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_ALL
            ]
        })
        self.assertEqual(scan_result.get(Metric.ROW_COUNT), 12)
        self.assertEqual(scan_result.get(Metric.SCHEMA), [{'name': 'score', 'type': 'character varying'}])
        self.assertEqual(scan_result.get(Metric.AVG_LENGTH), 1)
        self.assertEqual(scan_result.get(Metric.DISTINCT), 5)
        self.assertEqual(scan_result.get(Metric.DUPLICATE_COUNT), 3)
        self.assertCountEqual(scan_result.get(Metric.FREQUENT_VALUES), [{'frequency': 5, 'value': '3'},
                                                                        {'frequency': 2, 'value': '2'},
                                                                        {'frequency': 2, 'value': '4'},
                                                                        {'frequency': 1, 'value': '5'},
                                                                        {'frequency': 1, 'value': '1'}])
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT), 0)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE), 0)
        self.assertEqual(scan_result.get(Metric.MAXS), ['5', '4', '3', '2', '1'])
        self.assertEqual(scan_result.get(Metric.MAX_LENGTH), 1)
        self.assertEqual(scan_result.get(Metric.MINS), ['1', '2', '3', '4', '5'])
        self.assertEqual(scan_result.get(Metric.MIN_LENGTH), 1)
        self.assertEqual(scan_result.get(Metric.MISSING_COUNT), 1)
        self.assertEqual(scan_result.get(Metric.MISSING_PERCENTAGE), 8.333333333333334)
        self.assertEqual(scan_result.get(Metric.UNIQUENESS), 40)
        self.assertEqual(scan_result.get(Metric.UNIQUE_COUNT), 2)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT), 11)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE), 91.66666666666667)
        self.assertEqual(scan_result.get(Metric.VALUES_COUNT), 11)
        self.assertEqual(scan_result.get(Metric.VALUES_PERCENTAGE), 91.66666666666667)
        self.assertEqual(20, len(scan_result.measurements))

    def test_metric_group_all_number(self):
        self.sql_recreate_table(
            [f"score {self.dialect.data_type_integer}"],
            ["(1)",
             "(2)",
             "(2)",
             "(3)",
             "(3)",
             "(3)",
             "(3)",
             "(3)",
             "(4)",
             "(4)",
             "(5)",
             "(null)"])

        scan_result = self.scan({
            KEY_METRIC_GROUPS: [
                Metric.METRIC_GROUP_ALL
            ]
        })

        self.assertEqual(scan_result.get(Metric.ROW_COUNT), 12)
        self.assertEqual(scan_result.get(Metric.SCHEMA), [{'name': 'score', 'type': 'integer'}])
        self.assertEqual(scan_result.get(Metric.AVG), 3.0)
        self.assertEqual(scan_result.get(Metric.DISTINCT), 5)
        self.assertEqual(scan_result.get(Metric.DUPLICATE_COUNT), 3)
        self.assertCountEqual(scan_result.get(Metric.FREQUENT_VALUES), [{'frequency': 5, 'value': 3},
                                                                        {'frequency': 2, 'value': 2},
                                                                        {'frequency': 2, 'value': 4},
                                                                        {'frequency': 1, 'value': 5},
                                                                        {'frequency': 1, 'value': 1}])
        self.assertDictEqual(scan_result.get(Metric.HISTOGRAM), {'boundaries': [1.0,
                                                                                1.2,
                                                                                1.4,
                                                                                1.6,
                                                                                1.8,
                                                                                2.0,
                                                                                2.2,
                                                                                2.4,
                                                                                2.6,
                                                                                2.8,
                                                                                3.0,
                                                                                3.2,
                                                                                3.4,
                                                                                3.6,
                                                                                3.8,
                                                                                4.0,
                                                                                4.2,
                                                                                4.4,
                                                                                4.6,
                                                                                4.8,
                                                                                5.0],
                                                                 'frequencies': [1, 0, 0, 0, 0, 2, 0, 0, 0, 0, 5, 0, 0,
                                                                                 0, 0, 2, 0, 0, 0, 1]})
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT), 0)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE), 0)
        self.assertEqual(scan_result.get(Metric.MAX), 5)
        self.assertEqual(scan_result.get(Metric.MAXS), [5, 4, 3, 2, 1])
        self.assertEqual(scan_result.get(Metric.MIN), 1)
        self.assertEqual(scan_result.get(Metric.MINS), [1, 2, 3, 4, 5])
        self.assertEqual(scan_result.get(Metric.MISSING_COUNT), 1)
        self.assertEqual(scan_result.get(Metric.MISSING_PERCENTAGE), 8.333333333333334)
        self.assertAlmostEqual(scan_result.get(Metric.STDDEV), Decimal(1.09544), 4)
        self.assertEqual(scan_result.get(Metric.SUM), 33)
        self.assertEqual(scan_result.get(Metric.UNIQUENESS), 40)
        self.assertEqual(scan_result.get(Metric.UNIQUE_COUNT), 2)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT), 11)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE), 91.66666666666667)
        self.assertEqual(scan_result.get(Metric.VALUES_COUNT), 11)
        self.assertEqual(scan_result.get(Metric.VALUES_PERCENTAGE), 91.66666666666667)
        self.assertAlmostEqual(scan_result.get(Metric.VARIANCE), Decimal(1.2000), 4)
        self.assertEqual(24, len(scan_result.measurements))
