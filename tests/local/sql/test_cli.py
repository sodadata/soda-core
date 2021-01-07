#  Copyright 2020 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from pathlib import Path

from click.testing import CliRunner

from sodasql.cli.cli import main
from sodasql.cli.cli_impl import CliImpl
from tests.common.sql_test_case import SqlTestCase


class CliWithMockFileSystem(CliImpl):

    files = {}

    def file_read_as_str(self, path: Path) -> str:
        return self.files.get(str(path))

    def file_write_from_str(self, path: Path, file_content_str):
        self.files[str(path)] = file_content_str

    def file_exists(self, path: Path):
        return str(path) in self.files


class TestScan(SqlTestCase):

    original_cli = CliImpl.INSTANCE

    @classmethod
    def setUpClass(cls) -> None:
        CliImpl.INSTANCE = CliWithMockFileSystem()

    @classmethod
    def tearDownClass(cls) -> None:
        CliImpl.INSTANCE = cls.original_cli

    def test_scan_cli_invocation(self):
        runner = CliRunner()
        result = runner.invoke(main,
                               ['create', '-d', 'sodasql', '-u', 'sodasql', '-p', 'sodasql',
                                '/tmp/test_project', 'postgres'])
        self.assertEqual(result.exit_code, 0)
        for file_name in CliWithMockFileSystem.files:
            print(f'[{file_name}]')
            print(CliWithMockFileSystem.files.get(file_name))

        self.assertEqual(
            CliWithMockFileSystem.files['/tmp/test_project/warehouse.yml'],
            ('name: test_project\n'
             'connection:\n'
             '  type: postgres\n'
             '  host: localhost\n'
             '  username: env_var(POSTGRES_USERNAME)\n'
             '  password: env_var(POSTGRES_PASSWORD)\n'
             '  database: sodasql\n'
             '  schema: public\n'))

        self.assertEqual(
            CliWithMockFileSystem.files['/Users/tom/.soda/env_vars.yml'],
            ('test_project:\n'
             '  POSTGRES_USERNAME: sodasql\n'
             '  POSTGRES_PASSWORD: sodasql\n'))
