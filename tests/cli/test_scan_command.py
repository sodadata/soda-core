from unittest import TestCase
from unittest.mock import patch
from click.testing import CliRunner
import logging
from unittest import skip

from sodasql.cli import main
from .cli_base_test import BaseTestCase
from sodasql.cli.utils import ProfilesReader


class TestScan(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._create_test_table(["name VARCHAR(255)",
                                "size INTEGER"],
                               ["('one',    1)",
                                "('two',    2)",
                                "('three',  3)",
                                "(null,     null)"])

    def test_scan_command(self):
        with patch.object(ProfilesReader, 'PROFILES_FILE_PATH', self.test_profiles_file):
            runner = CliRunner()
            result = runner.invoke(main, ['scan',
                                          self._get_test_resource_path('test_scan_configuration'),
                                          self.test_warehouse,
                                          self.test_table])
            self._log_result(result)
            self.assertEqual(result.exit_code, 0)
