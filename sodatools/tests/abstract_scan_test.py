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
from typing import List, Optional
from unittest import TestCase

from sodatools.sql_store.sql_store import SqlStore
from sodatools.tests.logging_helper import LoggingHelper

LoggingHelper.configure_for_test()


class AbstractScanTest(TestCase):

    connection = None

    def __init__(self, method_name: str = ...) -> None:
        super().__init__(method_name)
        self.sql_store: Optional[SqlStore] = None
        self.connection = None

    def setUp(self) -> None:
        logging.debug(f'\n\n--- {str(self)} ---')
        super().setUp()
        self.sql_store = self.create_sql_store()
        self.connection = self.sql_store.get_connection()

    def create_sql_store(self) -> SqlStore:
        raise RuntimeError('Implement abstract method')

    def sql_update(self, sql: str):
        assert self.connection, 'self.connection not initialized'
        with self.connection.cursor() as cursor:
            logging.debug(f'Test SQL update: {sql}')
            return cursor.execute(sql)

    def sql_updates(self, sqls: List[str]):
        for sql in sqls:
            self.sql_update(sql)
