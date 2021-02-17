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
import json
import logging
from dataclasses import dataclass
from typing import Optional, List


@dataclass
class Test:

    id: str
    description: str
    expression: str
    metrics: List[str]
    column: Optional[str]

    def evaluate(self, test_variables: dict, group_values: Optional[dict] = None):
        from sodasql.scan.test_result import TestResult
        try:
            passed = bool(eval(self.expression, test_variables))
            values = {key: test_variables[key] for key in test_variables if key in self.metrics}
            test_result = TestResult(test=self, passed=passed, values=values, group_values=group_values)
            logging.debug(str(test_result))
            return test_result
        except Exception as e:
            return TestResult(test=self, passed=False, error=str(e), group_values=group_values)
