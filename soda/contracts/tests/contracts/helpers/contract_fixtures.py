import pytest

from contracts.helpers.test_connection import TestConnection
from helpers.data_source_fixture import DataSourceFixture
from soda.contracts.connection import Connection


@pytest.fixture(scope="session")
def test_connection(data_source_fixture: DataSourceFixture) -> Connection:
    test_connection = TestConnection(data_source_fixture)
    yield test_connection
