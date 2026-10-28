"""The org's blockGroupByResultsToCloud setting reaches the check collection, which turns it into the
redaction policy of its verification.

Group-by log sites only mark their records with ``RedactionCategory.GROUP_VALUES``; ``verify()`` sets the
``CheckCollectionImpl.redaction_policy`` built from that setting. So the setting must be the
one fetched for that verification (never one left over from an earlier run, and off without Soda Cloud), and
a marked record emitted during the verification must be redacted exactly when it is set.
"""

import logging
from typing import Optional

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.check_collections.base import CheckCollectionImpl
from soda_core.common.logging_constants import ExtraKeys, RedactionCategory
from soda_core.common.redaction import redacted_marker

MARKED_MESSAGE = "group ['SENTINEL']: 1"

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


@pytest.fixture
def marked_record_during_verify(monkeypatch) -> None:
    """Emit one record marked as group values from inside ``verify()``, the way a group-by log site does.

    ``_log_check_filters`` runs early in ``verify()`` on every collection; the marked record is emitted
    alongside it, so the test needs no group-by plugin."""
    original = CheckCollectionImpl._log_check_filters

    def log_check_filters_and_marked_record(self):
        original(self)
        logging.getLogger("soda_core.test").info(
            MARKED_MESSAGE, extra={ExtraKeys.REDACT: RedactionCategory.GROUP_VALUES}
        )

    monkeypatch.setattr(CheckCollectionImpl, "_log_check_filters", log_check_filters_and_marked_record)


def _captured_messages(check_collection: CheckCollectionImpl) -> list[str]:
    return [record.getMessage() for record in check_collection.logs.gatherer.get_all_logs()]


def _verify(data_source_test_helper: DataSourceTestHelper, test_table, block: Optional[bool]) -> None:
    if block is not None:
        data_source_test_helper.soda_cloud.set_block_group_by_results_to_cloud(block)
    data_source_test_helper.verify_contract(test_table=test_table, contract_yaml_str=CONTRACT_YAML)


def test_flag_reaches_check_collection(data_source_test_helper: DataSourceTestHelper, check_collections):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock()

    _verify(data_source_test_helper, test_table, block=True)

    assert [c.redaction_policy.covers(RedactionCategory.GROUP_VALUES) for c in check_collections] == [True]


def test_redaction_follows_latest_flag(data_source_test_helper: DataSourceTestHelper, check_collections):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock()

    _verify(data_source_test_helper, test_table, block=True)
    _verify(data_source_test_helper, test_table, block=False)

    assert [c.redaction_policy.covers(RedactionCategory.GROUP_VALUES) for c in check_collections] == [True, False]


def test_no_cloud_no_redaction(data_source_test_helper: DataSourceTestHelper, check_collections):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    assert data_source_test_helper.soda_cloud is None

    _verify(data_source_test_helper, test_table, block=None)

    assert [c.redaction_policy.covers(RedactionCategory.GROUP_VALUES) for c in check_collections] == [False]


def test_marked_record_redacted_when_flag_set(
    data_source_test_helper: DataSourceTestHelper, check_collections, marked_record_during_verify
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock()

    _verify(data_source_test_helper, test_table, block=True)

    messages = _captured_messages(check_collections[0])
    assert redacted_marker(RedactionCategory.GROUP_VALUES) in messages
    assert MARKED_MESSAGE not in messages


def test_marked_record_kept_when_flag_not_set(
    data_source_test_helper: DataSourceTestHelper, check_collections, marked_record_during_verify
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock()

    _verify(data_source_test_helper, test_table, block=False)

    messages = _captured_messages(check_collections[0])
    assert MARKED_MESSAGE in messages
    assert redacted_marker(RedactionCategory.GROUP_VALUES) not in messages
