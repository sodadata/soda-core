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
import logging
from dataclasses import dataclass
from typing import Optional


@dataclass
class Test:

    description: str
    expression: str
    first_metric: str
    column: Optional[str]

    def evaluate(self, test_variables: dict, group_values: Optional[dict] = None):
        from sodasql.scan.test_result import TestResult
        try:
            passed = bool(eval(self.expression, test_variables))
            value = test_variables.get(self.first_metric)
            logging.debug(f'Test {self.description} {"passed" if passed else "failed"}' +
                          (f" with group values {group_values}" if group_values else ''))

            return TestResult(test=self, passed=passed, value=value, group_values=group_values)
        except Exception as e:
            return TestResult(test=self, passed=False, error=str(e), group_values=group_values)
