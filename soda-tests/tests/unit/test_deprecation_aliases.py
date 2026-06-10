"""Tests that lock in the backwards-compatibility contract of the renamed agent → runner surfaces.

Every renamed symbol must keep its old name working and emit a single ``DeprecationWarning`` when
the old name is used.
"""
from __future__ import annotations

import pytest


def test_env_config_helper_is_running_on_agent_emits_deprecation_warning():
    """``EnvConfigHelper.is_running_on_agent`` is a deprecated alias for ``is_running_on_runner``;
    reading it must emit a DeprecationWarning and return the same value as the canonical property."""
    from soda_core.common.env_config_helper import EnvConfigHelper

    helper = EnvConfigHelper()
    canonical = helper.is_running_on_runner
    with pytest.warns(DeprecationWarning, match="is_running_on_agent"):
        legacy = helper.is_running_on_agent
    assert legacy == canonical


def test_soda_cloud_verify_contract_on_agent_emits_deprecation_warning(monkeypatch):
    """``SodaCloud.verify_contract_on_agent`` is a deprecated alias for ``verify_contract_on_runner``."""
    from soda_core.common.soda_cloud import SodaCloud

    called = {}

    def fake_runner(self, *args, **kwargs):
        called["called"] = True
        return "result"

    monkeypatch.setattr(SodaCloud, "verify_contract_on_runner", fake_runner)
    cloud = SodaCloud.__new__(SodaCloud)
    with pytest.warns(DeprecationWarning, match="verify_contract_on_agent"):
        result = cloud.verify_contract_on_agent()
    assert called["called"]
    assert result == "result"


def test_verify_contract_on_agent_function_emits_deprecation_warning(monkeypatch):
    """Module-level ``verify_contract_on_agent`` is a deprecated alias for ``verify_contract_on_runner``."""
    from soda_core.contracts.api import verify_api

    monkeypatch.setattr(verify_api, "verify_contract_on_runner", lambda **kwargs: "ok")
    with pytest.warns(DeprecationWarning, match="verify_contract_on_agent"):
        result = verify_api.verify_contract_on_agent(soda_cloud_file_path="sc.yaml")
    assert result == "ok"


def test_deprecated_kwarg_helper_returns_legacy_value_when_only_legacy_supplied():
    """The fixed helper must accept a single legacy kwarg without raising, even when the new
    param has a concrete default value at the callsite."""
    from soda_core.common._deprecation import deprecated_kwarg

    kwargs = {"old": True}
    with pytest.warns(DeprecationWarning, match="old"):
        # Caller's `new` param defaulted to False (concrete), which used to trip the conflict
        # check. With the sentinel-aware helper, this should now just return True.
        result = deprecated_kwarg(kwargs, "old", "new", current_new_value=None)
    assert result is True
    assert "old" not in kwargs


def test_deprecated_kwarg_helper_raises_on_conflicting_values():
    """If caller passes both legacy and canonical with conflicting non-None values, raise."""
    from soda_core.common._deprecation import deprecated_kwarg

    kwargs = {"old": True}
    with pytest.warns(DeprecationWarning, match="old"):
        with pytest.raises(TypeError, match="Cannot pass both old and new"):
            deprecated_kwarg(kwargs, "old", "new", current_new_value=False)


def test_deprecated_kwarg_helper_accepts_matching_values():
    """When legacy and canonical match, no error."""
    from soda_core.common._deprecation import deprecated_kwarg

    kwargs = {"old": True}
    with pytest.warns(DeprecationWarning, match="old"):
        result = deprecated_kwarg(kwargs, "old", "new", current_new_value=True)
    assert result is True


def test_deprecated_kwarg_helper_no_warning_when_legacy_absent():
    """No warning fires when caller didn't pass the legacy kwarg."""
    import warnings

    from soda_core.common._deprecation import deprecated_kwarg

    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter("always")
        result = deprecated_kwarg({}, "old", "new", current_new_value=False)
    assert result is False
    assert [r for r in records if issubclass(r.category, DeprecationWarning)] == []
