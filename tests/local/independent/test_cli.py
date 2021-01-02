from typing import Optional
from unittest import TestCase

from click.testing import CliRunner

from sodasql.cli import CLI, main
from sodasql.cli.cli import CliImpl


class MockCLI(CLI):

    invocations = []

    def create(self, project_dir: str, warehouse_type: str, profile: Optional[str] = 'default', target: Optional[str] = None):
        self.invocations.append(f'create {project_dir} {warehouse_type} {profile} {target}')

    def init(self, project_dir: str, profile: Optional[str] = 'default', target: Optional[str] = None):
        self.invocations.append(f'scan {project_dir} {profile} {target}')

    def scan(self, project_dir: str, table: str, profile: Optional[str] = 'default', target: Optional[str] = None):
        self.invocations.append(f'scan {project_dir} {table} {profile} {target}')

    def verify(self, project_dir: str, table: str, profile: Optional[str] = 'default', target: Optional[str] = None):
        self.invocations.append(f'verify {project_dir} {table} {profile} {target}')


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
        result = runner.invoke(main, ['scan', 'test_project', 'test_warehouse', 'test_table'])
        self.assertEqual(MockCLI.invocations[0], 'scan test_project test_warehouse test_table default')
        self.assertEqual(result.exit_code, 0)

