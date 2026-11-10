"""Direct unit tests for ``session_result_to_exit_code``.

The mapping is shared by every subtype CLI (``handle_verify_data_standards``,
future ``handle_verify_reconciliations``, ...). These tests lock the
precedence ordering — especially the multi-flag cases that the
subtype-level CLI tests don't cover one-flag-at-a-time.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from soda_core.cli.exit_codes import ExitCode, session_result_to_exit_code


@dataclass
class _StubRollups:
    """Minimal structural match for the ``_HasRollups`` Protocol."""

    has_errors: bool = False
    is_failed: bool = False
    is_warned: bool = False
    sending_results_to_soda_cloud_failed: bool = False


@pytest.mark.parametrize(
    "rollups, expected",
    [
        # Cloud-send failure trumps everything else.
        (
            _StubRollups(
                has_errors=True,
                is_failed=True,
                is_warned=True,
                sending_results_to_soda_cloud_failed=True,
            ),
            ExitCode.RESULTS_NOT_SENT_TO_CLOUD,
        ),
        # has_errors beats is_failed + is_warned when cloud is fine.
        (
            _StubRollups(has_errors=True, is_failed=True, is_warned=True),
            ExitCode.LOG_ERRORS,
        ),
        # is_failed beats is_warned when no errors / cloud OK.
        (
            _StubRollups(is_failed=True, is_warned=True),
            ExitCode.CHECK_FAILURES,
        ),
        # is_warned alone surfaces as CHECK_WARNINGS.
        (
            _StubRollups(is_warned=True),
            ExitCode.CHECK_WARNINGS,
        ),
        # Nothing set → OK.
        (_StubRollups(), ExitCode.OK),
    ],
)
def test_session_result_to_exit_code_precedence(rollups, expected):
    assert session_result_to_exit_code(rollups) == expected
