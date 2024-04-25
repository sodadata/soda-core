from typing import Any

import pytest
from contracts.helpers.test_warehouse import TestWarehouse
from helpers.data_source_fixture import DataSourceFixture
from soda.common.logs import configure_logging

from soda.contracts.impl.warehouse import Warehouse


def pytest_sessionstart(session: Any) -> None:
    configure_logging()
    # logging.getLogger("soda").setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def test_warehouse(data_source_fixture: DataSourceFixture) -> Warehouse:
    with TestWarehouse(data_source_fixture) as test_warehouse:
        yield test_warehouse
