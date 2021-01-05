from typing import Optional
from unittest import TestCase

from click.testing import CliRunner

from sodasql.cli import CLI, main
from sodasql.cli.cli import CliImpl


class MockCLI(CLI):

    invocations = []

    def create(self, project_dir: str, warehouse_type: str, profile: Optional[str] = None,
               target: Optional[str] = 'default_target'):
        pass

    def init(self, project_dir: str, profile: Optional[str] = 'default', target: Optional[str] = None):
        pass

    def scan(self, soda_project_dir: str, table: str, timeslice: Optional[str] = None,
             timeslice_variables: Optional[str] = None, target: Optional[str] = None) -> int:
        self.invocations.append(f'scan({soda_project_dir}, {table}, {timeslice}, {timeslice_variables}, {target})')

    def verify(self, project_dir: str, table: str, profile: Optional[str] = 'default', target: Optional[str] = None):
        pass


class TestScan(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.original_cli = CliImpl.cli
        CliImpl.cli = MockCLI()

    @classmethod
    def tearDownClass(cls) -> None:
        CliImpl.cli = cls.original_cli

    def test_scan_cli_invocation(self):
        runner = CliRunner()
        result = runner.invoke(main, ['scan', 'test_project', 'test_warehouse'])
        self.assertEqual(MockCLI.invocations[0], 'scan(test_project, test_warehouse, None, None, None)')
        self.assertEqual(result.exit_code, 0)

