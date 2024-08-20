from __future__ import annotations

import pytest
from contracts.helpers.contract_data_source_test_helper import (
    ContractDataSourceTestHelper,
)


@pytest.fixture(scope="session")
def data_source_test_helper() -> ContractDataSourceTestHelper:
    contract_data_source_test_helper = ContractDataSourceTestHelper.create()
    contract_data_source_test_helper.start_test_session()
    exception: Exception | None = None
    try:
        yield contract_data_source_test_helper
    except Exception as e:
        exception = e
    finally:
        contract_data_source_test_helper.end_test_session(exception=exception)
