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
