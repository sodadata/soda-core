from unittest import TestCase

from click.testing import CliRunner
from sodasql.cli import main


class TestScan(TestCase):

    def test_scan(self):
        runner = CliRunner()
        result = runner.invoke(main, ['scan', '/tmp', 'postgres', 'test_table'])
        self.assertEqual(result.exit_code, 0)
        self.assertIn('Scan dir=/tmp store=postgres table=test_table', result.output)
