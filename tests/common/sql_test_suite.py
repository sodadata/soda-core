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
import os
import unittest
from typing import Type
from unittest import TestLoader, TextTestRunner

from tests.common.sql_test_case import SqlTestCase


class TargetTestLoader(TestLoader):

    def __init__(self, target) -> None:
        super().__init__()
        self.target = target

    def loadTestsFromTestCase(self, testCaseClass: Type[unittest.case.TestCase]) -> unittest.suite.TestSuite:
        loaded_suite = super().loadTestsFromTestCase(testCaseClass)
        for test in loaded_suite._tests:
            if isinstance(test, SqlTestCase):
                test.target = self.target
        return loaded_suite


def run_test_suite(target: str):
    SqlTestCase.warehouses_close_enabled = False
    here = os.path.dirname(__file__)
    test_loader = TargetTestLoader(target)
    tests = test_loader.discover(start_dir=f'{here}/../local/sql')
    TextTestRunner().run(tests)
    SqlTestCase.teardown_close_warehouses()
