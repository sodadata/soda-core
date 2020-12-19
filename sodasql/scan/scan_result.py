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

from sodasql.scan.test_result import TestResult
from sodasql.scan.measurement import Measurement


class ScanResult:

    def __init__(self, measurements: List[Measurement], test_results: List[TestResult]):
        self.measurements: List[Measurement] = measurements
        self.test_results: List[TestResult] = test_results

    def has_failures(self):
        for test_result in self.test_results:
            if not test_result.passed:
                return True
        return False

    def find_measurement(self, metric_type: str, column_name: str = None):
        for measurement in self.measurements:
            if measurement.type == metric_type:
                if column_name is None or measurement.column == column_name:
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
