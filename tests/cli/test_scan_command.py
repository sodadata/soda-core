from unittest import TestCase
from unittest.mock import patch
from click.testing import CliRunner
import logging

from sodasql.cli import main
from .cli_base_test import BaseTestCase
from sodasql.cli.utils import ProfilesReader
from tests.common.sql_test_case import SqlTestCase


class TestScan(BaseTestCase, SqlTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.test_table = 'test_table'
        cls.test_warehouse = 'test_warehouse'

    def test_scan_command(self):
        with patch.object(ProfilesReader, 'PROFILES_FILE_PATH', self.test_profiles_file):
            self.sql_create_table(
                self.test_table,
                ["name VARCHAR(255)",
                 "size INTEGER"],
                ["('one',    1)",
                 "('two',    2)",
                 "('three',  3)",
                 "(null,     null)"])
            self.warehouse.connection.commit()

            runner = CliRunner()
            result = runner.invoke(main, ['scan',
                                          self._get_test_resource_path('test_scan_configuration'),
                                          self.test_warehouse,
                                          self.test_table])
            logging.debug(result)
            logging.debug(result.output)
            self.assertEqual(result.exit_code, 0)
