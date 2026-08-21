"""Unit tests for the DEBUG-formatting guard on ``SodaCloud._execute_cqrs_request``.

Building the request-body debug *text* costs a ``json.dumps(..., indent=2)`` plus
a token-masking regex substitution, run on *every* command/query request. Both
must be skipped entirely above DEBUG, not just have their result discarded.

``to_jsonnable(request_body)`` itself is a different matter: it mutates the body
in place (datetime/Decimal/Enum -> JSON-safe values), which the subsequent
``_http_post(json=request_body, ...)`` relies on to serialize at all — so unlike
the debug text, it must run on *every* request regardless of DEBUG/the flag below.
(This was a real bug: an earlier version of this guard skipped ``to_jsonnable``
too when DEBUG was disabled, so a raw ``datetime`` field would reach
``_http_post`` and blow up in ``json.dumps`` outside of a debug/verbose run —
fixed on the base branch, ahead of this one.)

R-657 task 1 adds a second gate on top of DEBUG for the debug *text* only: the
full request body (and, on failure, the full ``Response_text``) is the payload
firehose and only builds/logs when ``SODA_LOG_PAYLOADS`` is also set — DEBUG
alone now only gets the request name.
"""

import logging
from datetime import datetime, timezone

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


class _FailResponse:
    ok = False
    status_code = 400
    headers = {"X-Soda-Trace-Id": "trace-1"}
    text = "full failure body text"

    def json(self):
        return {"message": "bad request", "code": "invalid"}


def test_request_body_still_sanitized_when_debug_disabled(monkeypatch, caplog):
    """to_jsonnable's mutation is required for _http_post to serialize the request
    at all, so it must still run even when nothing gets logged."""
    caplog.set_level(logging.INFO, logger="soda")
    soda_cloud = _soda_cloud()
    monkeypatch.setattr(soda_cloud, "_http_post", lambda **kwargs: _OkResponse())
    to_jsonnable_spy_calls = []
    monkeypatch.setattr(
        "soda_core.common.soda_cloud.to_jsonnable",
        lambda *args, **kwargs: to_jsonnable_spy_calls.append((args, kwargs)) or {},
    )

    soda_cloud._execute_command({"type": "sodaCoreScanStart"}, request_log_name="scan_start")

    assert len(to_jsonnable_spy_calls) == 1
    assert [r for r in caplog.records if r.name == "soda" and "Sending command" in r.getMessage()] == []


def test_request_name_logged_when_debug_enabled_but_payloads_flag_off(monkeypatch, caplog):
    """Default (flag off): DEBUG shows the request name only, not the body — but
    sanitization (to_jsonnable) still runs since it's required for serialization."""
    caplog.set_level(logging.DEBUG, logger="soda")
    monkeypatch.delenv("SODA_LOG_PAYLOADS", raising=False)
    soda_cloud = _soda_cloud()
    monkeypatch.setattr(soda_cloud, "_http_post", lambda **kwargs: _OkResponse())
    to_jsonnable_spy_calls = []
    monkeypatch.setattr(
        "soda_core.common.soda_cloud.to_jsonnable",
        lambda *args, **kwargs: to_jsonnable_spy_calls.append((args, kwargs)) or {},
    )

    soda_cloud._execute_command({"type": "sodaCoreScanStart"}, request_log_name="scan_start")

    assert len(to_jsonnable_spy_calls) == 1
    debug_records = [r for r in caplog.records if r.name == "soda" and "Sending command scan_start" in r.getMessage()]
    assert len(debug_records) == 1
    assert [r for r in caplog.records if r.name == "soda" and "request body:" in r.getMessage()] == []


def test_request_body_serialized_when_debug_enabled_and_payloads_flag_on(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="soda")
    monkeypatch.setenv("SODA_LOG_PAYLOADS", "true")
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
    body_records = [r for r in caplog.records if r.name == "soda" and "request body:" in r.getMessage()]
    assert len(body_records) == 1


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


def test_token_masking_regex_not_run_when_debug_enabled_but_payloads_flag_off(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="soda")
    monkeypatch.delenv("SODA_LOG_PAYLOADS", raising=False)
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


def test_response_text_not_dumped_on_failure_when_payloads_flag_off(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="soda")
    monkeypatch.delenv("SODA_LOG_PAYLOADS", raising=False)
    soda_cloud = _soda_cloud()
    monkeypatch.setattr(soda_cloud, "_http_post", lambda **kwargs: _FailResponse())

    soda_cloud._execute_command({"type": "sodaCoreScanStart"}, request_log_name="scan_start")

    response_text_records = [r for r in caplog.records if r.name == "soda" and "Response_text:" in r.getMessage()]
    assert response_text_records == []


def test_datetime_field_reaches_http_post_as_a_string_when_debug_disabled(monkeypatch, caplog):
    """Regression guard: _http_post's json= kwarg goes through plain json.dumps,
    which can't serialize a raw datetime. to_jsonnable's mutation must reach the
    outgoing body regardless of DEBUG/the payloads flag."""
    caplog.set_level(logging.INFO, logger="soda")
    soda_cloud = _soda_cloud()
    post_calls = []
    monkeypatch.setattr(soda_cloud, "_http_post", lambda **kwargs: post_calls.append(kwargs) or _OkResponse())

    soda_cloud._execute_command(
        {"type": "sodaCoreScanStart", "createdAt": datetime(2025, 1, 1, tzinfo=timezone.utc)},
        request_log_name="scan_start",
    )

    assert len(post_calls) == 1
    assert isinstance(post_calls[0]["json"]["createdAt"], str)


def test_response_text_dumped_on_failure_when_payloads_flag_on(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger="soda")
    monkeypatch.setenv("SODA_LOG_PAYLOADS", "true")
    soda_cloud = _soda_cloud()
    monkeypatch.setattr(soda_cloud, "_http_post", lambda **kwargs: _FailResponse())

    soda_cloud._execute_command({"type": "sodaCoreScanStart"}, request_log_name="scan_start")

    response_text_records = [r for r in caplog.records if r.name == "soda" and "Response_text:" in r.getMessage()]
    assert len(response_text_records) == 1
    assert "full failure body text" in response_text_records[0].getMessage()
