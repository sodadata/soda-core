"""Unit tests for the memory_container plugin's inner-outcome parsing.

Two parsers exist: ``_inner_outcome_from_junit`` (the preferred, structured signal read from
pytest's JUnit XML — immune to ``-s`` stdout pollution) and ``_inner_run_skipped`` (the
stdout-summary fallback used only when the XML is missing/unparseable).
"""

from helpers.memory_container_plugin import (
    _inner_outcome_from_junit,
    _inner_run_skipped,
)


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


_SUITE = (
    '<testsuites><testsuite name="pytest" errors="{errors}" failures="{failures}" '
    'skipped="{skipped}" tests="{tests}"><testcase classname="m" name="t"/></testsuite></testsuites>'
)


def _write_junit(tmp_path, *, tests=1, skipped=0, failures=0, errors=0):
    path = tmp_path / "inner_junit.xml"
    path.write_text(_SUITE.format(tests=tests, skipped=skipped, failures=failures, errors=errors))
    return path


class TestInnerOutcomeFromJunit:
    def test_single_skipped_test(self, tmp_path):
        assert _inner_outcome_from_junit(_write_junit(tmp_path, tests=1, skipped=1)) == "skipped"

    def test_single_passed_test(self, tmp_path):
        assert _inner_outcome_from_junit(_write_junit(tmp_path, tests=1)) == "passed"

    def test_failure_reported(self, tmp_path):
        assert _inner_outcome_from_junit(_write_junit(tmp_path, tests=1, failures=1)) == "failed"

    def test_error_reported(self, tmp_path):
        assert _inner_outcome_from_junit(_write_junit(tmp_path, tests=1, errors=1)) == "error"

    def test_no_tests_collected_is_none(self, tmp_path):
        # Nothing ran — defer to rc / other gates rather than claiming an outcome.
        assert _inner_outcome_from_junit(_write_junit(tmp_path, tests=0)) is None

    def test_bare_testsuite_root_is_supported(self, tmp_path):
        path = tmp_path / "inner_junit.xml"
        path.write_text('<testsuite name="pytest" errors="0" failures="0" skipped="1" tests="1"/>')
        assert _inner_outcome_from_junit(path) == "skipped"

    def test_missing_file_is_none(self, tmp_path):
        # Triggers the stdout-scraping fallback in the caller.
        assert _inner_outcome_from_junit(tmp_path / "does_not_exist.xml") is None

    def test_unparseable_file_is_none(self, tmp_path):
        path = tmp_path / "inner_junit.xml"
        path.write_text("not xml <<<")
        assert _inner_outcome_from_junit(path) is None

    def test_skip_signal_is_immune_to_stdout_pollution(self, tmp_path):
        # A passing test whose body printed a fake "1 skipped" summary line would fool the
        # stdout scraper; the JUnit XML reports the real outcome (passed).
        polluted_stdout = "==================== 1 skipped in 0.01s ====================\n"
        assert _inner_run_skipped(polluted_stdout) is True  # the fallback IS fooled
        assert _inner_outcome_from_junit(_write_junit(tmp_path, tests=1)) == "passed"  # XML is not
