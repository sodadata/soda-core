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
from jinja2 import Template

logger = logging.getLogger(__name__)

@dataclass
class Test:
    id: str
    title: str
    expression: str
    metrics: List[str]
    column: Optional[str]
    expression_delimiters = ['<=', '>=', '<', '>', '==']

    def evaluate(self, test_variables: dict, group_values: Optional[dict] = None,
                 template_variables: Optional[dict] = None):
        from sodasql.scan.test_result import TestResult

        try:
            values = {key: test_variables[key] for key in test_variables if key in self.metrics}
            if template_variables is not None:
                self.expression = Template(self.expression).render(template_variables)
            if 'None' not in self.expression and any(v is None for v in values.values()):
                logger.warning(f'Skipping test {self.expression} since corresponding metrics are None ({values}) ')
                return TestResult(test=self, skipped=True, passed=True, values=values, group_values=group_values)
            else:
                passed = bool(eval(self.expression, test_variables))

                # Evaluate more complex expressions and save result of the expression.
                for delimiter in self.expression_delimiters:
                    if delimiter in self.expression:
                        left, _, _ = self.expression.partition(delimiter)
                        # Make sure the expression result is the first key in the resulting dict.
                        expression_result = {'expression_result': eval(left, test_variables)}
                        expression_result.update(values)
                        values = expression_result

                        break

                test_result = TestResult(test=self, passed=passed, skipped=False, values=values,
                                         group_values=group_values)
                logger.debug(str(test_result))
                return test_result
        except Exception as e:
            logger.error(f'Test error for "{self.expression}": {e}')
            return TestResult(test=self, passed=False, skipped=False, error=e, group_values=group_values)
