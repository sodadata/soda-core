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

from tests.warehouses.clstest.cls_test_case import ClsTestCase


class TestCls1(ClsTestCase):

    def get_database_configuration(self):
        return 'db1'

    def test_one_one(self):
        logging.debug(f'test one.one {str(self.database)}')

    def test_one_two(self):
        logging.debug(f'test one.two {str(self.database)}')

    @classmethod
    def tearDownClass(cls) -> None:
        logging.debug(f'tearDownClass 1 = {str(cls.database)}')
