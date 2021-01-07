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
from typing import List, Optional, Set


@dataclass
class Test:

    expression: str
    first_metric: str
    column: Optional[str]
    sql_metric: Optional[str]

    def evaluate(self, test_variables: dict):
        from sodasql.scan.test_result import TestResult
        try:
            passed = bool(eval(self.expression, test_variables))
            value = test_variables.get(self.first_metric)
            return TestResult(self, passed, value, None, self.column)
        except Exception as e:
            return TestResult(self, False, None, str(e), self.column)
