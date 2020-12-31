from unittest import TestCase
import os
import logging
from unittest.mock import patch

from sodasql.cli.utils import ScanConfigurationReader
from .cli_base_test import BaseTestCase


class TestScanConfigurationReader(BaseTestCase):

    def test_scan_configuration_reader(self):
        reader = ScanConfigurationReader('test_warehouse',
                                         'test_table',
                                         self._get_test_resource_path('test_project'))
        self._assert_logs(reader.parse_logs)
        self.assertListEqual(sorted(list(reader.configuration.metrics)), ['max_length', 'min_length'])
