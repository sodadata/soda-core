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
import random
import re
import socket
import string
from typing import Optional

from tests.local.sql.test_distinct_and_uniqueness import TestDistinctAndUniqueness
from tests.local.sql.test_frequent_values import TestFrequentValues
from tests.local.sql.test_histogram_numeric import TestHistogramNumeric
from tests.local.sql.test_min_max_length import TestMinMaxLength
from tests.local.sql.test_mins_maxs import TestMinsMaxs
from tests.local.sql.test_missing_and_invalid_customizations import TestMissingAndInvalidCustomizations
from tests.local.sql.test_missing_and_invalid_metric_configurations import TestMissingAndInvalidMetricConfigurations
from tests.local.sql.test_schema import TestSchema
from tests.local.sql.test_statistical_metrics import TestStatisticalMetrics
from tests.local.sql.test_tests import TestTests


class AllWarehouseTests(
        TestDistinctAndUniqueness,
        TestFrequentValues,
        TestHistogramNumeric,
        TestMinMaxLength,
        TestMinsMaxs,
        TestMissingAndInvalidCustomizations,
        TestMissingAndInvalidMetricConfigurations,
        TestSchema,
        TestStatisticalMetrics,
        TestTests):

    def __init__(self, method_name: str = ...) -> None:
        super().__init__(method_name)
        self.database: Optional[str] = None

    def setup_get_warehouse_configuration(self, profile_name: str, profile_target_name: str):
        warehouse_configuration = super().setup_get_warehouse_configuration(profile_name, profile_target_name)
        self.database = self.init_create_unique_database_name('soda_test')
        warehouse_configuration['database'] = self.database
        return warehouse_configuration

    def init_create_unique_database_name(self, prefix: str):
        normalized_hostname = re.sub(r"(?i)[^a-zA-Z0-9]", "_", socket.gethostname()).lower()
        random_suffix = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))
        return f"{prefix}_{normalized_hostname}_{random_suffix}"
