import pytest

from contracts.helpers.test_data_source import DataSourceTestHelper
from soda.contracts.impl.data_source import DataSource


@pytest.fixture(scope="session")
def test_data_source() -> DataSource:
    data_source_test_helper: DataSourceTestHelper = DataSourceTestHelper.create()
    with data_source_test_helper:
        yield data_source_test_helper
