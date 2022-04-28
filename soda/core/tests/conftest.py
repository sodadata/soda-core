from __future__ import annotations

import logging
from typing import Any

import pytest
from soda.common.file_system import FileSystemSingleton
from soda.common.logs import configure_logging
from soda.telemetry.soda_telemetry import SodaTelemetry

# Initialize telemetry in test mode. This is done before importing Scanner which initializes telemetry in standard mode so that we avoid unnecessary setup and re-setup which causes errors.
soda_telemetry = SodaTelemetry.get_instance(test_mode=True)

from tests.helpers.mock_file_system import MockFileSystem
from tests.helpers.scanner import Scanner

logger = logging.getLogger(__name__)


def pytest_sessionstart(session: Any) -> None:
    configure_logging()


def pytest_runtest_logstart(nodeid: str, location: tuple[str, int | None, str]) -> None:
    """
    Prints the test function name and the location in a format that PyCharm recognizes and turns into a link in the console
    """
    logging.debug(f'### "soda/core/tests/{location[0]}:{(location[1]+1)}" {location[2]}')


@pytest.fixture(scope="session")
def data_source_config_str() -> str:
    project_root_dir = __file__[: -len("soda/core/tests/conftest.py")]
    with open(f"{project_root_dir}soda/core/tests/data_sources.yml") as f:
        return f.read()


@pytest.fixture(scope="session")
def scanner(data_source):
    yield Scanner(data_source)


@pytest.fixture
def mock_file_system():
    """
    Fixture that swaps the file_system() with an in-memory, mock version
    Usage:

    def test_mytest_with_mock_file_system(mock_file_system):
        mock_file_system.files = {
          '~/.soda/configuration.yml': '...content of the file...',
          '/project/mytable.soql.yml': '...content of the file...'
        }

        # Now file system will use the given the in memory string files above instead of using the real file system
        file_system().exists('~/.soda/configuration.yml') -> True
    """
    original_file_system = FileSystemSingleton.INSTANCE
    FileSystemSingleton.INSTANCE = MockFileSystem()
    yield FileSystemSingleton.INSTANCE
    FileSystemSingleton.INSTANCE = original_file_system
