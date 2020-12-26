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
from typing import List


def sql_fetchone(connection, sql: str) -> tuple:
    cursor = connection.cursor()
    try:
        logging.debug(f'Executing SQL query: \n{sql}')
        start = datetime.now()
        cursor.execute(sql)
        row_tuple = cursor.fetchone()
        delta = datetime.now() - start
        logging.debug(f'SQL took {str(delta)}')
        return row_tuple
    finally:
        cursor.close()


def sql_fetchall(connection, sql: str) -> List[tuple]:
    cursor = connection.cursor()
    try:
        logging.debug(f'Executing SQL query: \n{sql}')
        start = datetime.now()
        cursor.execute(sql)
        rows = cursor.fetchall()
        delta = datetime.now() - start
        logging.debug(f'SQL took {str(delta)}')
        return rows
    finally:
        cursor.close()


def sql_update(connection, sql: str):
    cursor = connection.cursor()
    try:
        logging.debug(f'Executing SQL update: \n{sql}')
        start = datetime.now()
        cursor.execute(sql)
        delta = datetime.now() - start
        logging.debug(f'SQL took {str(delta)}')
    finally:
        cursor.close()


def sql_updates(connection, sqls: List[str]):
    for sql in sqls:
        sql_update(connection, sql)
