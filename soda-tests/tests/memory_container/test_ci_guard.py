"""The memory_container plugin must refuse to run in CI — local-only policy.

The guard sits BEFORE the SODA_MEMORY_TESTS activation check, so even a CI
job that exports the activation env var gets skips, never docker dispatch.
"""

from helpers.memory_container_plugin import _running_in_ci


class TestRunningInCi:
    def test_github_actions_detected(self, monkeypatch):
        monkeypatch.delenv("CI", raising=False)
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        assert _running_in_ci() is True

    def test_generic_ci_detected(self, monkeypatch):
        monkeypatch.setenv("CI", "1")
        monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
        assert _running_in_ci() is True

    def test_local_machine_not_ci(self, monkeypatch):
        monkeypatch.delenv("CI", raising=False)
        monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
        assert _running_in_ci() is False

    def test_falsey_values_not_ci(self, monkeypatch):
        monkeypatch.setenv("CI", "false")
        monkeypatch.setenv("GITHUB_ACTIONS", "0")
        assert _running_in_ci() is False
