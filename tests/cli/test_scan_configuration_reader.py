from sodasql.cli.utils import ScanConfigurationReader
from tests.cli.cli_base_test import BaseTestCase


class TestScanConfigurationReader(BaseTestCase):

    def test_scan_configuration_reader(self):
        reader = ScanConfigurationReader('test_warehouse',
                                         'test_table',
                                         self._get_test_resource_path('test_project'))
        reader.parse_logs.log()
        reader.parse_logs.assert_no_warnings_or_errors('Test scan.yml')
        self.assertListEqual(sorted(list(reader.configuration.metrics)), ['max_length', 'min_length'])
