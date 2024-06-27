from typing import Any

import pytest
from contracts.helpers.test_data_source import TestDataSource
from helpers.data_source_fixture import DataSourceFixture
from soda.common.logs import configure_logging

from soda.contracts.impl.data_source import DataSource


# def pytest_sessionstart(session: Any) -> None:
#     configure_logging()
#     # logging.getLogger("soda").setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def test_data_source(data_source_fixture: DataSourceFixture) -> DataSource:
    with TestDataSource(data_source_fixture) as test_data_source:
        yield test_data_source
