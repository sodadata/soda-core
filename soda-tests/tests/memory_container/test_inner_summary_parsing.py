"""Unit tests for the memory_container plugin's inner-summary skip parsing."""

from helpers.memory_container_plugin import _inner_run_skipped


class TestInnerRunSkipped:
    def test_pure_skip_summary_detected(self):
        stdout = "collected 1 item\n\n==================== 1 skipped in 0.01s ====================\n"
        assert _inner_run_skipped(stdout) is True

    def test_pass_summary_not_skipped(self):
        stdout = "==================== 1 passed, 1 warning in 9.30s ====================\n"
        assert _inner_run_skipped(stdout) is False

    def test_mixed_pass_and_skip_not_treated_as_skip(self):
        # A passed test alongside a skipped one means real work ran —
        # the measurement is genuine.
        stdout = "============ 1 passed, 1 skipped in 5.00s ============\n"
        assert _inner_run_skipped(stdout) is False

    def test_failed_summary_not_skipped(self):
        stdout = "=========== 1 failed, 1 skipped in 2.00s ===========\n"
        assert _inner_run_skipped(stdout) is False

    def test_no_summary_line(self):
        assert _inner_run_skipped("garbage output, container died early") is False

    def test_last_summary_line_wins(self):
        # Sub-process pytest output (e.g. from nested tooling) may contain
        # earlier summary lines; only the final one describes this run.
        stdout = (
            "==================== 1 passed in 1.00s ====================\n"
            "some trailing logs\n"
            "==================== 1 skipped in 0.01s ====================\n"
        )
        assert _inner_run_skipped(stdout) is True
