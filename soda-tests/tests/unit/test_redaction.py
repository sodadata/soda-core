"""``RedactionPolicy``: one policy object for every output, log record or not.

How a policy redacts log records is covered in test_logging_masking.py, through ``_mask_record``.
"""

from __future__ import annotations

import logging

import pytest
from soda_core.common import logging_configuration
from soda_core.common.logging_constants import ExtraKeys, RedactionCategory
from soda_core.common.logs import Logs
from soda_core.common.redaction import RedactionPolicy, redacted_marker
from soda_core.common.soda_cloud_dto import DatasetConfigurationsDTO

GROUP_VALUES = RedactionCategory.GROUP_VALUES
MARKER = redacted_marker(GROUP_VALUES)
BLOCKING = RedactionPolicy(frozenset({GROUP_VALUES}))
OPEN = RedactionPolicy()


def _raised(message: str) -> RuntimeError:
    try:
        raise RuntimeError(message)
    except RuntimeError as e:
        return e


def _configurations(block: bool | None) -> DatasetConfigurationsDTO:
    return DatasetConfigurationsDTO(dataset_configurations=[], block_group_by_results_to_cloud=block)


@pytest.mark.parametrize(
    "configs, expected",
    [
        (_configurations(True), BLOCKING),
        (_configurations(False), OPEN),
        (_configurations(None), OPEN),
        (None, OPEN),
    ],
)
def test_policy_from_org_settings(configs, expected) -> None:
    assert RedactionPolicy.from_datasets_configurations(configs) == expected


def test_activate_yields_the_effective_policy() -> None:
    with BLOCKING.activate():
        with OPEN.activate() as effective:
            assert effective == BLOCKING
            assert RedactionPolicy.current() == BLOCKING
    assert RedactionPolicy.current() == OPEN


def test_redact_text() -> None:
    assert BLOCKING.redact("group SENTINEL", GROUP_VALUES) == MARKER
    assert BLOCKING.redact("group SENTINEL", "something_else") == "group SENTINEL"
    assert OPEN.redact("group SENTINEL", GROUP_VALUES) == "group SENTINEL"


def test_redact_exception_keeps_only_the_class_when_anything_is_blocked() -> None:
    assert BLOCKING.redact_exception(_raised("group SENTINEL")) == f"{MARKER} RuntimeError"


def test_redact_exception_is_the_stack_trace_when_nothing_is_blocked() -> None:
    stack_trace = OPEN.redact_exception(_raised("group SENTINEL"))

    assert "Traceback" in stack_trace and "SENTINEL" in stack_trace


def _captured(policy: RedactionPolicy) -> logging.LogRecord:
    logs = Logs()
    with logs.activate():
        policy.log_exception(logging.getLogger("soda_core.test"), "handler failed for SENTINEL", _raised("SENTINEL"))
    [record] = logs.get_log_records()
    return record


def test_log_exception_redacts_without_an_active_policy() -> None:
    """It activates its own policy, so it holds after the collection's verify() has returned."""
    record = _captured(BLOCKING)

    assert record.getMessage() == MARKER
    assert getattr(record, ExtraKeys.EXCEPTION) == "RuntimeError"
    assert RedactionPolicy.current() == OPEN


def test_log_exception_keeps_the_error_when_nothing_is_blocked() -> None:
    record = _captured(OPEN)

    assert record.getMessage() == "handler failed for SENTINEL"
    assert record.exc_info is not None


def test_first_cut_aliases_follow_the_policy() -> None:
    """soda-extensions still imports these from logging_configuration."""
    assert not logging_configuration.is_redacted(GROUP_VALUES)
    with logging_configuration.redaction_policy({GROUP_VALUES}):
        assert logging_configuration.is_redacted(GROUP_VALUES)
        assert RedactionPolicy.current() == BLOCKING
    assert logging_configuration.redacted_marker is redacted_marker
