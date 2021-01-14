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
from typing import AnyStr

import pytest
from click.testing import CliRunner
from sodasql.cli.cli import main
from sodasql.cli.file_system import FileSystem, FileSystemSingleton
from sodasql.scan.env_vars import EnvVars
from tests.common.sql_test_case import SqlTestCase


class CliWithMockFileSystem(FileSystem):

    dirs = set()
    files = {}

    def user_home_dir(self):
        return '/Users/johndoe'

    def is_dir(self, path: AnyStr):
        return path in self.dirs

    def mkdirs(self, path: AnyStr):
        self.dirs.add(path)

    def file_read_as_str(self, path: Path) -> str:
        return self.files.get(str(path))

    def file_write_from_str(self, path: Path, file_content_str):
        self.files[str(path)] = file_content_str

    def file_exists(self, path: Path):
        return str(path) in self.files

    def is_file(self, path: AnyStr):
        return path in self.files

    def is_readable(self, path: AnyStr):
        return self.is_file(path)

    def list_dir(self, dir_path):
        return {k: v for k, v in self.files.items() if k.startswith(dir_path)}


class TestScan(SqlTestCase):

    original_file_system = FileSystemSingleton.INSTANCE

    def setUp(self) -> None:
        super().setUp()
        FileSystemSingleton.INSTANCE = CliWithMockFileSystem()

    @classmethod
    def tearDownClass(cls) -> None:
        FileSystemSingleton.INSTANCE = cls.original_file_system

    def test_scan_cli_create_invocation(self):
        runner = CliRunner()
        result = runner.invoke(main,
                               ['create', '-d', 'sodasql', '-u', 'sodasql', '-p', 'sodasql',
                                '~/test_project', 'postgres'])
        self.assertEqual(result.exit_code, 0)
        for file_name in CliWithMockFileSystem.files:
            print(f'[{file_name}]')
            print(CliWithMockFileSystem.files.get(file_name))

        self.assertEqual(
            CliWithMockFileSystem.files['~/test_project/warehouse.yml'],
            ('name: test_project\n'
             'connection:\n'
             '  type: postgres\n'
             '  host: localhost\n'
             '  username: env_var(POSTGRES_USERNAME)\n'
             '  password: env_var(POSTGRES_PASSWORD)\n'
             '  database: sodasql\n'
             '  schema: public\n'))

        self.assertEqual(
            CliWithMockFileSystem.files[f'{FileSystemSingleton.INSTANCE.user_home_dir()}/.soda/env_vars.yml'],
            ('test_project:\n'
             '  POSTGRES_USERNAME: sodasql\n'
             '  POSTGRES_PASSWORD: sodasql\n'))

    @pytest.mark.skip(reason="no way of currently testing this until we prefill the database with a table")
    def test_scan_cli_scan_invocation(self):
        FileSystemSingleton.INSTANCE.mkdirs('./test_project')
        FileSystemSingleton.INSTANCE.file_write_from_str('./test_project/warehouse.yml', (
            'name: test_project\n'
             'connection:\n'
             '  type: postgres\n'
             '  host: localhost\n'
             '  username: env_var(POSTGRES_USERNAME)\n'
             '  password: env_var(POSTGRES_PASSWORD)\n'
             '  database: sodasql\n'
             '  schema: public\n'
        ))
        FileSystemSingleton.INSTANCE.file_write_from_str(f'{FileSystemSingleton.INSTANCE.user_home_dir()}/.soda/env_vars.yml', (
            'test_project:\n'
             '  POSTGRES_USERNAME: sodasql\n'
             '  POSTGRES_PASSWORD: sodasql\n'
        ))
        FileSystemSingleton.INSTANCE.mkdirs('./test_project/postgres')
        FileSystemSingleton.INSTANCE.file_write_from_str('./test_project/postgres/scan.yml', (
            'table_name: postgres\n'
            'metrics:\n'
            '  - row_count\n'
            'tests:\n'
            '  must have rows: row_count > 0\n'
        ))

        EnvVars.load_env_vars('test_project')
        runner = CliRunner()
        result = runner.invoke(main,
                               ['scan', './test_project', 'postgres'])
        self.assertEqual(result.exit_code, 0)
