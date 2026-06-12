"""Lock the plugin's runtime-skip detection against regression.

Static skip/skipif markers are evaluated before docker dispatch (see
test_skip_marker_honored.py), but a test body calling ``pytest.skip(...)``
AT RUNTIME inside the container used to phantom-pass: the inner pytest
exits 0 with '1 skipped', and the plugin reported a pass whose peak was
just the container import floor. The plugin now parses the inner summary
line and reports SKIPPED.

Regression mechanics (the established xfail(strict) trick): with the fix,
this test reports SKIPPED — green. If the fix regresses, the plugin emits
a phantom pass and ``xfail(strict=True)`` turns it into a loud failure.
"""

import pytest


@pytest.mark.memory_container(limit_mb=256)
@pytest.mark.xfail(
    strict=True,
    reason=(
        "The body skips at runtime; the plugin must report SKIPPED. If the "
        "inner-summary parsing regresses, the phantom pass becomes a strict "
        "xpass failure."
    ),
)
def test_runtime_skip_is_reported_as_skipped():
    pytest.skip("deliberate runtime skip: validates the plugin's inner-summary skip detection")
