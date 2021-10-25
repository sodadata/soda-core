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
from deprecated import deprecated

from sodasql.scan.measurement import Measurement
from sodasql.scan.scan_error import ScanError
from sodasql.scan.test_result import TestResult


class ScanResult:

    def __init__(self):
        self.measurements: List[Measurement] = []
        self.test_results: List[TestResult] = []
        self.error = None

        # Any scan error (bug or sql syntax problems) should
        #  - be logged at the end of the scan on the console
        #  - be sent to the Soda Cloud (if connected and if authentication was successful)
        #  - make the scan fail
        self.errors: List[ScanError] = []

    def add_error(self, error: ScanError):
        self.errors.append(error)

    def add_test_results(self, test_results):
        self.test_results.extend(test_results)

    # TODO rename to has_test_failures()
    def has_test_failures(self) -> bool:
        return self.get_test_failures_count() > 0

    # TODO rename to get_test_failures_count()
    def get_test_failures_count(self) -> int:
        test_failures = self.get_test_failures()
        return len(test_failures)

    def get_test_failures(self) -> List[TestResult]:
        return [test_result for test_result in self.test_results if not test_result.passed]

    def has_errors(self) -> bool:
        """
        DEPRECATED keep, but change impl to self.scan_errors
        """
        return self.get_errors_count() > 0

    def get_errors(self) -> List[ScanError]:
        return self.errors

    def get_errors_count(self) -> int:
        return len(self.errors)

    def is_passed(self) -> bool:
        return not (self.has_test_failures() or self.has_errors())

    def to_dict(self) -> dict:
        return {
            'measurements': [measurement.to_dict() for measurement in self.measurements],
            'testResults': [test_result.to_dict() for test_result in self.test_results],
            'errors': [scan_error.to_dict() for scan_error in self.errors]
        }

    @deprecated(version='2.1.0b19', reason='This function is deprecated, please use to_dict')
    def to_json(self):
        return self.to_dict()

    def find_measurement(self, metric_type: str, column_name: str = None) -> Measurement:
        """
        Returns the measurement or None if the measurement does not exist
        """
        for measurement in self.measurements:
            if measurement.metric == metric_type:
                if column_name is None or measurement.column_name.lower() == column_name.lower():
                    return measurement

    def find(self, metric_type: str, column_name: str = None):
        """
        Returns the measurement value or None if the measurement does not exist
        """
        measurement = self.find_measurement(metric_type, column_name)
        return measurement.value if measurement else None

    def get_measurement(self, metric_type: str, column_name: str = None) -> Measurement:
        """
        Returns the measurement or raises AssertionError if the measurement does not exist
        """
        measurement = self.find_measurement(metric_type, column_name)
        if measurement is None:
            raise AssertionError(
                f'No measurement found for metric {metric_type}' +
                (f' and column {column_name}' if column_name else '') + '\n' +
                '\n'.join([str(m) for m in self.measurements]))
        return measurement

    def get(self, metric_type: str, column_name: str = None):
        """
        Returns the measurement value or raises AssertionError if the measurement does not exist
        """
        return self.get_measurement(metric_type, column_name).value
