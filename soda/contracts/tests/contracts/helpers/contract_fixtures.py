import pytest

from contracts.helpers.test_data_source import DataSourceTestHelper
from soda.contracts.impl.data_source import DataSource


@pytest.fixture(scope="session")
def data_source() -> DataSource:
    data_source: DataSource = DataSource.create()
    with data_source:
        yield data_source


@pytest.fixture(scope="session")
def data_source_test_helper(data_source: DataSource) -> DataSourceTestHelper:
    data_source_test_helper: DataSourceTestHelper = DataSourceTestHelper.create(data_source)
    yield data_source_test_helper
