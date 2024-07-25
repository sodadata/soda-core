import pytest

from contracts.helpers.test_data_source import ContractDataSourceTestHelper


@pytest.fixture(scope="session")
def data_source_test_helper() -> ContractDataSourceTestHelper:
    with ContractDataSourceTestHelper.create() as data_source_test_helper:
        yield data_source_test_helper
