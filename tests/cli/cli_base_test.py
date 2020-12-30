from unittest import TestCase
import os
import logging
from unittest.mock import patch

from sodasql.cli.utils.profiles_reader import ProfilesReader
from sodasql.scan import parse_logs


class BaseTestCase(TestCase):
    _resources_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resources')

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.test_profiles_file = cls._get_test_resource_path('profiles.yml')

    @classmethod
    def _get_test_resource_path(cls, resource):
        return os.path.join(cls._resources_folder, resource)

    def _assert_logs(self, logs):
        self._log_messages(logs)
        self.assertFalse(logs.has_warnings_or_errors(),
                         f"Operation logs have warnings or errors.")

    @staticmethod
    def _log_messages(logs):
        for log in logs.logs:
            if log.level in [parse_logs.ERROR, parse_logs.WARNING]:
                logging.error(log)
            else:
                logging.info(log)
