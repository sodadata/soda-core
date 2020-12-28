from unittest import TestCase

from click.testing import CliRunner
from sodasql.cli.cli import soda_sql_cli


class TestScan(TestCase):

    def test_scan(self):
        runner = CliRunner()
        result = runner.invoke(soda_sql_cli, ['scan', '/tmp', 'postgres', 'test_table'])
        self.assertEqual(result.exit_code, 0)
        self.assertIn('Scan dir=/tmp store=postgres table=test_table', result.output)
