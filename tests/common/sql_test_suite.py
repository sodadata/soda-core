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

from tests.local.sql.test_distinct_and_uniqueness import TestDistinctAndUniqueness
from tests.local.sql.test_frequent_values import TestFrequentValues
from tests.local.sql.test_histogram_numeric import TestHistogramNumeric
from tests.local.sql.test_min_max_length import TestMinMaxLength
from tests.local.sql.test_mins_maxs import TestMinsMaxs
from tests.local.sql.test_missing_and_invalid_customizations import TestMissingAndInvalidCustomizations
from tests.local.sql.test_missing_and_invalid_metric_configurations import TestMissingAndInvalidMetricConfigurations
from tests.local.sql.test_schema import TestSchema
from tests.local.sql.test_statistical_metrics import TestStatisticalMetrics
from tests.local.sql.test_tests_table_metric import TestTestsTableMetric
from tests.local.sql.test_numeric_data import TestNumericData


class SqlTestSuite(
        TestDistinctAndUniqueness,
        TestFrequentValues,
        TestHistogramNumeric,
        TestMinMaxLength,
        TestMinsMaxs,
        TestMissingAndInvalidCustomizations,
        TestMissingAndInvalidMetricConfigurations,
        TestSchema,
        TestStatisticalMetrics,
        TestTestsTableMetric,
        TestNumericData):

    def setUp(self) -> None:
        if type(self) == SqlTestSuite:
            # Ensuring that AllSqlTests is not executed as a test class
            # but that subclasses do execute the common test methods in this class
            self.skipTest('Abstract test class should not execute its test methods, only subclasses.')
        else:
            super().setUp()
            self.warehouses_close_enabled = False

    def tearDown(self) -> None:
        super().tearDown()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.teardown_close_warehouses()
