from typing import Any

import pytest
from contracts.helpers.test_connection import TestConnection
from helpers.data_source_fixture import DataSourceFixture
from soda.common.logs import configure_logging

from soda.contracts.connection import Connection


def pytest_sessionstart(session: Any) -> None:
    configure_logging()
    # logging.getLogger("soda").setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def test_connection(data_source_fixture: DataSourceFixture) -> Connection:
    test_connection = TestConnection(data_source_fixture)
    yield test_connection
