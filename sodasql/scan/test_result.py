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
from dataclasses import dataclass
from typing import Optional, Any

from sodasql.scan.test import Test


@dataclass
class TestResult:

    test: Test
    passed: bool
    value: Optional[Any] = None
    error: Optional[str] = None
    group_values: Optional[dict] = None

    def to_json(self):
        if not self.test or not self.test.expression:
            return {
                'error': 'Invalid test result'
            }
        if self.error:
            return {
                'test': self.test.expression,
                'error': error
            }
        test_result_json = {
            'test': self.test.expression,
            'passed': self.passed,
            'value': self.value
        }
        if self.group_values:
            test_result_json['groupValues'] = self.group_values
        return test_result_json
