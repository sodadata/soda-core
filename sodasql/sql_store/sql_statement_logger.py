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
from datetime import datetime


class SqlStatementLogger:

    def __init__(self, sql: str):
        logging.debug(f'Executing SQL query: \n{sql}')
        self.start = datetime.now()

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, exc_traceback):
        delta = datetime.now() - self.start
        logging.debug(f'SQL took {str(delta)}')


def log_sql(sql: str):
    return SqlStatementLogger(sql)
