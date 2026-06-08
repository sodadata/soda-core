"""Lock the memory_container plugin's handler-error detection against regression.

soda's verification handlers (e.g. ``failed_rows_extractor``) are invoked
from ``check_collections/base.py`` inside a broad ``try/except`` that
catches any exception, logs it via ``logger.error(..., exc_info=True)``,
and continues. The inner pytest sees the contract complete and reports
the test as passing — but the workload the memory test meant to measure
aborted partway through, so the measured peak is meaningless.

The plugin's ``_detect_swallowed_handler_error`` scans the inner stdout
for soda's literal error marker after the container exits. If it finds
the marker on a test that otherwise reports ``"passed"``, it overrides
the outcome to ``"failed"`` with the captured traceback as the reason.

This file exercises that detection without depending on a real soda
contract or DB connection — we synthesize the exact marker shape that
``base.py`` would emit (``Error in <handler> verification handler: <msg>``
followed by a ``Traceback`` block from ``exc_info=True``). The test body
asserts nothing, so without detection the plugin would report ``passed``;
``xfail(strict=True)`` then flips that "unexpected pass" into a suite
failure, surfacing detection regressions loudly. With detection working,
the plugin returns ``failed`` → xfail(strict) sees the expected failure
→ suite stays green.
"""

import sys

import pytest


@pytest.mark.memory_container(limit_mb=128)
@pytest.mark.xfail(
    strict=True,
    reason=(
        "Emits the soda verification-handler error marker to validate the "
        "memory_container plugin's post-container error detection. The test "
        "body itself does not fail — without detection the plugin would "
        "report passed, and xfail(strict=True) would flip that into an "
        "unexpected-pass failure. With detection working, the plugin "
        "reports failed and xfail consumes the expected failure."
    ),
)
def test_swallowed_verification_handler_error_is_caught():
    # Emit the EXACT shape that soda-core's check_collections/base.py logs
    # when a verification handler raises. The plugin scans for this pattern
    # in the inner stdout post-exit (`Error in <name> verification handler`
    # is the literal prefix from logger.error at base.py:928).
    print(
        "Error in synthetic_contract.yml verification handler: "
        "RuntimeError('synthetic — exercises memory_container plugin detection')",
        flush=True,
    )
    # logger.error(..., exc_info=True) follows the message with a real
    # Python traceback. Reproduce that shape so the detector's snippet
    # extraction has something to capture.
    print("Traceback (most recent call last):", flush=True)
    print('  File "synthetic.py", line 1, in <module>', flush=True)
    print(
        "    raise RuntimeError('synthetic — exercises memory_container plugin detection')",
        flush=True,
    )
    print(
        "RuntimeError: synthetic — exercises memory_container plugin detection",
        flush=True,
    )
    sys.stdout.flush()
    # Deliberately no assertion. The detection logic is what must fail
    # this test — if the body completed without it, the plugin would
    # report passed and xfail(strict) would catch the regression.
