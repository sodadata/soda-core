"""Transport errors between soda-core and Soda Cloud.

A dropped connection, a TLS error or a timeout says nothing about Soda Cloud's health, so the same
request is sent again a few times. A command that Soda Cloud may already have processed is only sent
again when it provably never left this machine.
"""

import logging
import socket
import threading
from time import monotonic as real_monotonic
from typing import Iterator, Union
from unittest import mock

import pytest
import requests
import urllib3
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from soda_core.common import soda_cloud as soda_cloud_module
from soda_core.common.soda_cloud import (
    TRANSPORT_RETRY_DELAYS_SECONDS,
    SodaCloud,
    failed_before_sending,
    is_idempotent_command,
)

_MAX_ATTEMPTS: int = len(TRANSPORT_RETRY_DELAYS_SECONDS) + 1


@pytest.fixture
def waits(monkeypatch) -> list[float]:
    """Swaps sleep for one that only records the wait, and takes the top of the jitter range."""
    recorded: list[float] = []
    monkeypatch.setattr(soda_cloud_module, "sleep", recorded.append)
    monkeypatch.setattr(soda_cloud_module.random, "uniform", lambda low, high: high)
    return recorded


def _ssl_eof() -> requests.exceptions.SSLError:
    return requests.exceptions.SSLError("[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol")


def _connection_refused() -> requests.exceptions.ConnectionError:
    reason = urllib3.exceptions.NewConnectionError(None, "Failed to establish a new connection: [Errno 111]")
    return requests.exceptions.ConnectionError(urllib3.exceptions.MaxRetryError(None, "/api/command", reason))


class _FlakyCloud(MockSodaCloud):
    """Raises the queued errors from _http_post, then answers from the queued responses."""

    def __init__(self, outcomes: list[Union[Exception, MockResponse]]):
        super().__init__(responses=[o for o in outcomes if not isinstance(o, Exception)])
        self.outcomes: Iterator[Union[Exception, MockResponse]] = iter(outcomes)
        self.attempts: int = 0

    def _http_post(self, request_log_name=None, url=None, headers=None, json=None, data=None):
        self.attempts += 1
        outcome = next(self.outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return super()._http_post(request_log_name=request_log_name, url=url, headers=headers, json=json, data=data)


def _scan_started() -> MockResponse:
    return MockResponse(status_code=200, json_object={"scanReference": "org/ref-1"})


def _scan_start(soda_cloud: SodaCloud):
    return soda_cloud.scan_start(scan_id="scan-1", definition_name="my_scan", default_data_source="postgres")


def _messages(caplog, level: int) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == level]


def test_scan_start_is_sent_again_after_an_ssl_eof_and_succeeds(waits, caplog):
    caplog.set_level(logging.INFO)
    cloud = _FlakyCloud([_ssl_eof(), _scan_started()])

    assert _scan_start(cloud) == "org/ref-1"

    assert cloud.attempts == 2
    assert waits == [pytest.approx(TRANSPORT_RETRY_DELAYS_SECONDS[0] * 1.3)]
    assert not _messages(caplog, logging.ERROR) and not _messages(caplog, logging.CRITICAL)
    assert any("sending it again" in m for m in _messages(caplog, logging.INFO))


def test_scan_start_gives_up_after_the_configured_attempts_within_a_minute(waits, caplog):
    cloud = _FlakyCloud([_ssl_eof()] * _MAX_ATTEMPTS)

    assert _scan_start(cloud) is None

    assert cloud.attempts == _MAX_ATTEMPTS
    assert sum(waits) < 60
    errors = _messages(caplog, logging.ERROR)
    assert len(errors) == 1 and f"giving up after {_MAX_ATTEMPTS} attempts" in errors[0]
    # A transport failure is reported at error, not critical: Soda Cloud rejects critical log records.
    assert not _messages(caplog, logging.CRITICAL)


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(requests.exceptions.ReadTimeout("read timed out"), id="read timeout"),
        pytest.param(requests.exceptions.ChunkedEncodingError("connection broken"), id="broken answer"),
        pytest.param(requests.exceptions.ConnectionError("connection reset by peer"), id="reset"),
    ],
)
def test_a_query_is_sent_again_on_any_transport_error(waits, error):
    cloud = _FlakyCloud([error, MockResponse(status_code=200, json_object={"ok": True})])

    response = cloud._execute_query({"type": "whoAmI"}, request_log_name="who_am_i")

    assert response is not None and response.status_code == 200
    assert cloud.attempts == 2


def test_a_results_upload_is_not_sent_again_when_the_connection_broke_after_it_may_have_arrived(waits, caplog):
    cloud = _FlakyCloud([_ssl_eof(), MockResponse(status_code=200)])

    assert cloud.insert_scan_data_batch({"definitionName": "my_scan"}, scan_reference="org/ref-1") is False

    assert cloud.attempts == 1
    assert waits == []
    errors = _messages(caplog, logging.ERROR)
    assert len(errors) == 1 and "may already have processed it" in errors[0]


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(_connection_refused(), id="connection refused"),
        pytest.param(requests.exceptions.ConnectTimeout("connect timed out"), id="connect timeout"),
    ],
)
def test_a_results_upload_is_sent_again_when_it_never_left_this_machine(waits, error):
    cloud = _FlakyCloud([error, MockResponse(status_code=200)])

    assert cloud.insert_scan_data_batch({"definitionName": "my_scan"}, scan_reference="org/ref-1") is True

    assert cloud.attempts == 2


def test_login_is_sent_again_after_a_transport_error(waits):
    cloud = _FlakyCloud([_ssl_eof(), MockResponse(status_code=200, json_object={"token": "fresh"}), _scan_started()])
    cloud.token = None

    assert _scan_start(cloud) == "org/ref-1"
    assert cloud.token == "fresh"


@pytest.mark.parametrize(
    "error, expected",
    [
        pytest.param(_connection_refused(), True, id="refused"),
        pytest.param(requests.exceptions.ConnectTimeout("connect timed out"), True, id="connect timeout"),
        pytest.param(_ssl_eof(), False, id="tls error: may be after the body was sent"),
        pytest.param(requests.exceptions.ReadTimeout("read timed out"), False, id="read timeout"),
        pytest.param(requests.exceptions.ConnectionError("connection aborted"), False, id="aborted"),
        pytest.param(requests.exceptions.ChunkedEncodingError("broken"), False, id="broken answer"),
    ],
)
def test_failed_before_sending(error, expected):
    assert failed_before_sending(error) is expected


@pytest.fixture
def silent_server() -> Iterator[str]:
    """A server that accepts connections and reads requests but never answers."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(8)
    connections: list[socket.socket] = []

    def accept_forever():
        while True:
            try:
                connection, _ = server.accept()
            except OSError:
                return
            connections.append(connection)

    threading.Thread(target=accept_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.getsockname()[1]}"
    server.close()
    for connection in connections:
        connection.close()


def test_a_request_that_never_answers_ends_at_the_read_timeout(silent_server, monkeypatch):
    monkeypatch.setattr(soda_cloud_module, "SODA_CLOUD_READ_TIMEOUT_SECONDS", 0.3)
    cloud = SodaCloud(host="unused", api_key_id="id", api_key_secret="secret", token="t", port=None, scheme="https")

    started = real_monotonic()
    with pytest.raises(requests.exceptions.ReadTimeout):
        cloud._http_post(url=f"{silent_server}/api/command", json={"type": "sodaCoreScanStart"})

    assert real_monotonic() - started < 5


def test_every_request_carries_a_connect_and_a_read_timeout():
    cloud = SodaCloud(host="unused", api_key_id="id", api_key_secret="secret", token="t", port=None, scheme="https")
    expected = (soda_cloud_module.SODA_CLOUD_CONNECT_TIMEOUT_SECONDS, soda_cloud_module.SODA_CLOUD_READ_TIMEOUT_SECONDS)

    with mock.patch.object(soda_cloud_module.requests, "post") as post, mock.patch.object(
        soda_cloud_module.requests, "get"
    ) as get:
        cloud._http_post(url="https://cloud/api/command", json={})
        cloud._http_get(url="https://cloud/api/v1/scans/1")

    assert post.call_args.kwargs["timeout"] == expected
    assert get.call_args.kwargs["timeout"] == expected


@pytest.mark.parametrize(
    "command, expected",
    [
        pytest.param({"type": "sodaCoreScanStart", "scanId": "scan-1"}, True, id="start of a pre-created scan"),
        pytest.param({"type": "sodaCoreScanStart"}, False, id="start without scanId creates a new scan"),
        pytest.param({"type": "sodaCoreMarkScanFailed", "scanId": "scan-1"}, True, id="mark failed"),
        pytest.param({"type": "sodaCoreInsertScanDataBatch"}, False, id="results batch would be ingested twice"),
        pytest.param({"type": "sodaCoreScanEndAsync"}, False, id="scan end would ingest again"),
        pytest.param({"type": "sodaCoreInsertScanResults"}, False, id="sync results upload creates a scan"),
        pytest.param({"type": "sodaCoreVerifyContract"}, False, id="verify submits a scan"),
    ],
)
def test_is_idempotent_command(command, expected):
    assert is_idempotent_command(command) is expected
