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
import os
import uuid
from pathlib import Path
from typing import AnyStr

import pytest
from click.testing import CliRunner

from sodasql.cli.cli import main
from sodasql.cli.file_system import FileSystem, FileSystemSingleton
from sodasql.dialects.postgres_dialect import PostgresDialect
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

    def file_read_as_str(self, path: AnyStr) -> str:
        return self.files.get(str(path))

    def file_write_from_str(self, path: AnyStr, file_content_str):
        self.files[str(path)] = file_content_str

    def file_exists(self, path: AnyStr):
        return str(path) in self.files

    def is_file(self, path: AnyStr):
        return path in self.files

    def is_readable(self, path: AnyStr):
        return self.is_file(path)

    def list_dir(self, dir_path):
        return {k: v for k, v in self.files.items() if k.startswith(dir_path)}


class TestScan(SqlTestCase):

    original_file_system = FileSystemSingleton.INSTANCE
    mock_file_system = None
    demodata_table_name = 'demodata'

    def setUp(self) -> None:
        super().setUp()
        FileSystemSingleton.INSTANCE = CliWithMockFileSystem()
        self.mock_file_system = FileSystemSingleton.INSTANCE

    @classmethod
    def tearDownClass(cls) -> None:
        FileSystemSingleton.INSTANCE = cls.original_file_system

    def test_cli_tutorial_scenario(self):
        if isinstance(self.warehouse.dialect, PostgresDialect):
            self.load_demo_data()
            self.run_cli_create()
            self.run_cli_init()
            self.run_cli_scan()

    def load_demo_data(self):
        demodata_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'demo', 'demodata.sql'))
        with open(demodata_path) as f:
            sql_statements_str = f.read()
            for sql_statement in sql_statements_str.split(';'):
                if sql_statement.strip():
                    self.sql_update(sql_statement)
        self.warehouse.connection.commit()

    def run_cli_create(self):
        runner = CliRunner()
        result = runner.invoke(main, ['create', '-d', 'sodasql', '-u', 'sodasql', '-p', 'sodasql', './test_project',
                                      'postgres'])
        self.assertEqual(result.exit_code, 0)
        for file_name in self.mock_file_system.files:
            print(f'[{file_name}]')
            print(CliWithMockFileSystem.files.get(file_name))

        self.assertIsNotNone(self.mock_file_system.files.get('./test_project/warehouse.yml'))
        user_home_dir = self.mock_file_system.user_home_dir()
        self.assertIsNotNone(self.mock_file_system.files.get(f'{user_home_dir}/.soda/env_vars.yml'))

    def run_cli_init(self):
        runner = CliRunner()
        result = runner.invoke(main, ['init', './test_project'])
        self.assertEqual(result.exit_code, 0)

        scan_yml_str = self.mock_file_system.file_read_as_str(f'./test_project/{self.demodata_table_name}/scan.yml')
        self.assertIsNotNone(scan_yml_str)
        print(scan_yml_str)

    def run_cli_scan(self):
        runner = CliRunner()
        result = runner.invoke(main, ['scan', './test_project', self.demodata_table_name])
        self.assertEqual(result.exit_code, 0)

