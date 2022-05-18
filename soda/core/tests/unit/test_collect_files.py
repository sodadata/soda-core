from __future__ import annotations

from typing import List

from soda.scan import Scan
from tests.conftest import project_root_dir

basedir = f"{project_root_dir}soda/core/tests/unit/test_collect_files_dir"


def collect_paths(path: str, recursive: bool | None = True, suffix: str | None = ".yml") -> list[str]:
    scan = Scan()
    return scan._collect_file_paths(path=path, recursive=recursive, suffix=suffix)


def assert_paths(actual_paths, expected_file_paths):
    prefix_length = len(basedir)
    relative_actual_paths = [path[prefix_length:] for path in actual_paths]
    assert relative_actual_paths == expected_file_paths


def test_collect_files():
    assert_paths(
        collect_paths(f"{basedir}/cfgs"),
        [
            "/cfgs/nested/nested/nested_configuration.yml",
            "/cfgs/nested/nested/nested_configuration2.yml",
            "/cfgs/root_configuration.yml",
        ],
    )


def test_collect_files_slash():
    assert_paths(
        collect_paths(f"{basedir}/sodacls/"),
        [
            "/sodacls/nested/nested_checks.yml",
            "/sodacls/root_checks.yml",
            "/sodacls/root_checks2.yml",
        ],
    )


def test_collect_files_empty():
    assert_paths(collect_paths(f"{basedir}/empty"), [])
