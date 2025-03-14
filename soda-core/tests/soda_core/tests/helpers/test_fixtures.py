from __future__ import annotations

import os
from typing import Optional

import pytest
from dotenv import load_dotenv
from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper

project_root_dir = __file__[: -len("/soda-core/tests/soda_core/tests/helpers/test_fixtures.py")]
load_dotenv(f"{project_root_dir}/.env", override=True)


@pytest.fixture(scope="function")
def env_vars() -> dict:
    original_env_vars = dict(os.environ)
    yield os.environ
    os.environ.clear()
    os.environ.update(original_env_vars)


@pytest.fixture(scope="function")
def data_source_test_helper(data_source_test_helper_session: DataSourceTestHelper) -> DataSourceTestHelper:
    yield data_source_test_helper_session
    data_source_test_helper_session.test_method_ended()


@pytest.fixture(scope="session")
def data_source_test_helper_session() -> DataSourceTestHelper:
    data_source_test_helper: DataSourceTestHelper = DataSourceTestHelper.create()
    data_source_test_helper.start_test_session()
    exception: Optional[Exception] = None
    try:
        yield data_source_test_helper
    except Exception as e:
        exception = e
    finally:
        data_source_test_helper.end_test_session(exception=exception)
