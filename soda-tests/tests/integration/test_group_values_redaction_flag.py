"""The org's blockGroupByResultsToCloud setting reaches the check collection that redacts group values.

Group-by log sites read ``CheckCollectionImpl.block_group_by_results_to_cloud`` to mark their records and
to enable ``group_values_scope``, so the setting must be the one fetched for that verification: never one
left over from an earlier run, and off without Soda Cloud.
"""

from typing import Optional

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.check_collections.base import CheckCollectionImpl

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("group_values_redaction_flag")
    .column_varchar("country", 3)
    .rows(rows=[("BE",), ("US",)])
    .build()
)

CONTRACT_YAML = """
    checks:
      - row_count:
"""


@pytest.fixture
def check_collections(monkeypatch) -> list[CheckCollectionImpl]:
    """Every CheckCollectionImpl the test builds, in construction order."""
    built: list[CheckCollectionImpl] = []
    original_init = CheckCollectionImpl.__init__

    def recording_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        built.append(self)

    monkeypatch.setattr(CheckCollectionImpl, "__init__", recording_init)
    return built


def _verify(data_source_test_helper: DataSourceTestHelper, test_table, block: Optional[bool]) -> None:
    if block is not None:
        data_source_test_helper.soda_cloud.set_block_group_by_results_to_cloud(block)
    data_source_test_helper.verify_contract(test_table=test_table, contract_yaml_str=CONTRACT_YAML)


def test_flag_reaches_check_collection(data_source_test_helper: DataSourceTestHelper, check_collections):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock()

    _verify(data_source_test_helper, test_table, block=True)

    assert [c.block_group_by_results_to_cloud for c in check_collections] == [True]


def test_redaction_follows_latest_flag(data_source_test_helper: DataSourceTestHelper, check_collections):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock()

    _verify(data_source_test_helper, test_table, block=True)
    _verify(data_source_test_helper, test_table, block=False)

    assert [c.block_group_by_results_to_cloud for c in check_collections] == [True, False]


def test_no_cloud_no_redaction(data_source_test_helper: DataSourceTestHelper, check_collections):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    assert data_source_test_helper.soda_cloud is None

    _verify(data_source_test_helper, test_table, block=None)

    assert [c.block_group_by_results_to_cloud for c in check_collections] == [False]
