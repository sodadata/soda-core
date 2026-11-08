"""Parametrize soda-core's arch tests with the dialects it ships."""

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper


def pytest_generate_tests(metafunc):
    if "arch_sql_dialect" not in metafunc.fixturenames:
        return

    sql_dialects = DataSourceTestHelper.create_registered_sql_dialects()
    metafunc.parametrize(
        "arch_sql_dialect",
        [pytest.param(sql_dialect, id=label) for label, sql_dialect in sql_dialects],
    )
