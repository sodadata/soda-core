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
from unittest import TestCase

from tests.common.logging_helper import LoggingHelper

LoggingHelper.configure_for_test()


class ClsTestCase(TestCase):

    database = None

    def setUp(self) -> None:
        super().setUp()

        database_configuration = self.get_database_configuration()
        if self.database is not None \
                and self.database['configuration'] != database_configuration:
            self.database = None
        if self.database is None:
            self.database = self.create_database(database_configuration)
            self.cache_database(self.database)

    def get_database_configuration(self):
        pass

    def create_database(self, database_configuration):
        database = {
            'configuration': database_configuration
        }
        logging.debug(f'created database {database_configuration}')
        return database

    def cache_database(self, database):
        ClsTestCase.database = database
