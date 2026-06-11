"""Lock the memory_container plugin's skip-marker honoring against regression.

The plugin replaces the test protocol at ``pytest_runtest_protocol``, which
runs BEFORE pytest's skipping plugin would evaluate ``skip``/``skipif``
markers. Until 2026-06-11 the plugin dispatched skipif'd tests to docker
anyway: the inner pytest (same env) skipped them and exited 0, and the
plugin reported a phantom "pass" whose peak was just the container's
~150 MB import floor — observed on all 13 ``test_scaling_profiling_only``
variants in every full-suite run.

Regression mechanics (same trick as ``test_handler_error_detection``):
with the fix, the always-true ``skipif`` below wins and the test reports
SKIPPED — green. If the fix regresses, the test is dispatched, the inner
pytest skips it, the plugin reports "passed", and ``xfail(strict=True)``
flips that unexpected pass into a loud suite failure. The body can never
legitimately run in either scenario.
"""

import pytest


@pytest.mark.memory_container(limit_mb=128)
@pytest.mark.skipif(True, reason="always-skip: validates skip markers are honored before docker dispatch")
@pytest.mark.xfail(
    strict=True,
    reason=(
        "Must never be reached: the skipif above wins when the plugin honors "
        "skip markers. If dispatch happens anyway (regression), the inner "
        "pytest skips and the plugin emits a phantom pass — xfail(strict) "
        "turns that into a failure."
    ),
)
def test_skipif_is_honored_before_docker_dispatch():
    raise AssertionError("body must never execute — the skipif marker must win before dispatch")
