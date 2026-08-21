"""Unit tests for the DEBUG-formatting guard on ``SodaCloud._execute_cqrs_request``.

Building the request-body debug line costs a ``json.dumps(..., indent=2)`` plus
a token-masking regex substitution, run on *every* command/query request. Both
must be skipped entirely above DEBUG, not just have their result discarded.
"""

import logging

from soda_core.common.soda_cloud import SodaCloud


def _soda_cloud() -> SodaCloud:
    # A pre-set token skips the login round-trip through _http_post, so only
    # the request under test hits the (patched) transport.
    return SodaCloud(
        host="cloud.soda.io", api_key_id="id", api_key_secret="secret", token="preset-token", port=None, scheme="https"
    )


class _OkResponse:
    ok = True
    status_code = 200
    headers = {"X-Soda-Trace-Id": "trace-1"}


def test_request_body_not_serialized_when_debug_disabled(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="soda")
    soda_cloud = _soda_cloud()
    monkeypatch.setattr(soda_cloud, "_http_post", lambda **kwargs: _OkResponse())
    to_jsonnable_spy_calls = []
    monkeypatch.setattr(
        "soda_core.common.soda_cloud.to_jsonnable",
        lambda *args, **kwargs: to_jsonnable_spy_calls.append((args, kwargs)) or {},
    )

    soda_cloud._execute_command({"type": "sodaCoreScanStart"}, request_log_name="scan_start")

    assert to_jsonnable_spy_calls == []


def test_request_body_serialized_when_debug_enabled(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="soda")
    soda_cloud = _soda_cloud()
    monkeypatch.setattr(soda_cloud, "_http_post", lambda **kwargs: _OkResponse())
    to_jsonnable_spy_calls = []

    def spy(*args, **kwargs):
        to_jsonnable_spy_calls.append((args, kwargs))
        return {}

    monkeypatch.setattr("soda_core.common.soda_cloud.to_jsonnable", spy)

    soda_cloud._execute_command({"type": "sodaCoreScanStart"}, request_log_name="scan_start")

    assert len(to_jsonnable_spy_calls) == 1
    debug_records = [r for r in caplog.records if r.name == "soda" and "Sending command scan_start" in r.getMessage()]
    assert len(debug_records) == 1


def test_token_masking_regex_not_run_when_debug_disabled(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="soda")
    soda_cloud = _soda_cloud()
    monkeypatch.setattr(soda_cloud, "_http_post", lambda **kwargs: _OkResponse())
    clean_spy_calls = []
    monkeypatch.setattr(
        SodaCloud,
        "_clean_request_from_private_info",
        lambda self, json_str: clean_spy_calls.append(json_str) or json_str,
    )

    soda_cloud._execute_command({"type": "sodaCoreScanStart"}, request_log_name="scan_start")

    assert clean_spy_calls == []
