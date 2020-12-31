import logging
import os
import traceback
from typing import List
from unittest import TestCase
from unittest.mock import patch

from click.testing import CliRunner

from sodasql.cli import main
from sodasql.cli.utils import ProfilesReader
from sodasql.scan import db
from sodasql.scan import parse_logs
from sodasql.scan.warehouse import Warehouse


class BaseTestCase(TestCase):
    _resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resources')

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.test_profiles_file = cls._get_test_resource_path('profiles.yml')
        cls.test_table = 'test_table'
        cls.test_warehouse = 'test_warehouse'
        cls._create_warehouse()

    @classmethod
    def tearDownClass(cls):
        cls._drop_test_table()
        cls.warehouse.close()

    @classmethod
    def _create_warehouse(cls):
        with patch.object(ProfilesReader, 'PROFILES_FILE_PATH', cls.test_profiles_file):
            profile_reader = ProfilesReader()
            cls.warehouse = Warehouse(profile_reader.configuration)
            cls.warehouse.connection.autocommit = True

    @classmethod
    def _get_test_resource_path(cls, resource):
        return os.path.join(cls._resources_folder, resource)

    def _assert_logs(self, logs):
        self._log_messages(logs)
        self.assertFalse(logs.has_warnings_or_errors(), f"Test logs have warnings or errors.")

    @staticmethod
    def _log_messages(logs):
        for log in logs.logs:
            if log.level in [parse_logs.ERROR, parse_logs.WARNING]:
                logging.error(log)
            else:
                logging.info(log)

    @staticmethod
    def _log_result(result):
        logging.info(result.output)
        if result.exception:
            logging.error(''.join(traceback.format_exception(etype=type(result.exception),
                                                             value=result.exception,
                                                             tb=result.exception.__traceback__)))

    @classmethod
    def _create_test_table(cls, columns: List[str], rows: List[str]):
        joined_columns = ", ".join([cls.warehouse.dialect.qualify_column_name(column) for column in columns])
        joined_rows = ", ".join(rows)
        db.sql_updates(cls.warehouse.connection,
                       [f"DROP TABLE IF EXISTS {cls.warehouse.dialect.qualify_table_name(cls.test_table)}",
                        f"CREATE TABLE {cls.test_table} ({joined_columns})",
                        f"INSERT INTO {cls.test_table} VALUES {joined_rows}"])

    @classmethod
    def _drop_test_table(cls):
        db.sql_update(cls.warehouse.connection,
                      f"DROP TABLE IF EXISTS {cls.warehouse.dialect.qualify_table_name(cls.test_table)}")

    def _execute_cli(self, parameters):
        with patch.object(ProfilesReader, 'PROFILES_FILE_PATH', self.test_profiles_file):
            runner = CliRunner()
            result = runner.invoke(main, parameters)
            self._log_result(result)
        return result
