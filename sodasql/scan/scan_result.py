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
from typing import List, Optional

from sodasql.scan.measurement import Measurement
from sodasql.scan.test_result import TestResult


class ScanResult:

    def __init__(self, timeslice: Optional[str] = None):
        self.timeslice: Optional[str] = timeslice
        self.measurements: List[Measurement] = []
        self.test_results: List[TestResult] = []

    def has_failures(self):
        return self.failures_count() > 0

    def failures_count(self):
        failures_count = 0
        for test_result in self.test_results:
            if not test_result.passed:
                failures_count += 1
        return failures_count

    def find_measurement(self, metric_type: str, column_name: str = None):
        for measurement in self.measurements:
            if measurement.metric == metric_type:
                if column_name is None or measurement.column_name.lower() == column_name.lower():
                    return measurement

    # get measurement value and raise exception if the measurement does not exist
    def find(self, metric_type: str, column_name: str = None):
        measurement = self.find_measurement(metric_type, column_name)
        return measurement.value if measurement else None

    # get measurement value and raise exception if the measurement does not exist
    def get(self, metric_type: str, column_name: str = None):
        return self.get_measurement(metric_type, column_name).value

    def get_measurement(self, metric_type: str, column_name: str = None):
        measurement = self.find_measurement(metric_type, column_name)
        if measurement is None:
            raise AssertionError(
                f'No measurement found for metric {metric_type}' +
                (f' and column {column_name}' if column_name else '') + '\n' +
                '\n'.join([str(m) for m in self.measurements]))
        return measurement
