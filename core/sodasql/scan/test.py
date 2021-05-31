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
from typing import Optional, List


@dataclass
class Test:
    id: str
    title: str
    expression: str
    metrics: List[str]
    column: Optional[str]

    def evaluate(self, test_variables: dict, group_values: Optional[dict] = None):
        from sodasql.scan.test_result import TestResult

        try:
            values = {key: test_variables[key] for key in test_variables if key in self.metrics}
            if 'None' not in self.expression and any(v is None for v in values.values()):
                logging.warning(f'Skipping test {self.expression} since corresponding metrics are None ({values}) ')
                return TestResult(test=self, skipped=True, passed=True, values=values, group_values=group_values)
            else:
                passed = bool(eval(self.expression, test_variables))
                test_result = TestResult(test=self, passed=passed, skipped=False, values=values,
                                         group_values=group_values)
                logging.debug(str(test_result))
                return test_result
        except Exception as e:
            logging.error(f'Test error for "{self.expression}": {e}')
            return TestResult(test=self, passed=False, skipped=False, error=e, group_values=group_values)
