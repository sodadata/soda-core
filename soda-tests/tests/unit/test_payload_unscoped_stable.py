"""Pin what Soda Cloud receives for an unscoped contract.

Each snapshot was taken before any scope code existed, so the scope work can prove that an unscoped contract
still sends what it sent before. A snapshot holds two things: the requests sent to Soda Cloud, in order, and the
``sodaCoreInsertScanResults`` payload without its logs. The payload carries each check's path, identities,
definition, attributes, outcome and diagnostics. The logs stay out because they hold debug output and query
text, which change for reasons that have nothing to do with what Soda Cloud stores per check.

The test contract is the orders fixture in ``helpers.orders_contract``. Only values that change between runs are
masked: the scan id and the scan start and end timestamps.

A run filtered by check path and one filtered by a check selector are pinned the same way, each in a
snapshot of its own, so the payload of a partial run, EXCLUDED checks included, stays the same too.

A scoped variant of the contract declares two scopes and adds scoped copies of three checks. Its unscoped checks must
upload exactly what the snapshot holds, but for their line numbers.

To update the snapshots after an intended change, run with ``SODA_TEST_UPDATE_SNAPSHOTS=1`` and review the
diff before committing it.
"""

from __future__ import annotations

import copy
import difflib
import json
from pathlib import Path
from urllib.parse import urlparse

import pytest
from helpers.mock_soda_cloud import MockSodaCloud
from helpers.orders_contract import (
    CONTRACT_YAML,
    scoped_contract_yaml,
    uploaded_payloads,
    verify_contract,
    verify_session,
)
from helpers.scopes_extension_removal import without_scopes_extension  # noqa: F401
from helpers.snapshot_updates import UPDATE_SNAPSHOTS_ENV_VAR, updating_snapshots
from soda_core.contracts.impl.check_selector import CheckSelector

SNAPSHOT_PATH = Path(__file__).parent / "snapshots" / "scan_results_payload_unscoped.json"
MASK = "<masked>"


def _verify_unscoped_contract(
    monkeypatch, check_paths: list[str] | None = None, check_selectors: list[CheckSelector] | None = None
) -> dict:
    """The requests sent to Soda Cloud, in order, and the uploaded results payload."""
    _, soda_cloud = verify_session(
        monkeypatch, [CONTRACT_YAML], check_paths=check_paths, check_selectors=check_selectors
    )
    payloads: list[dict] = uploaded_payloads(soda_cloud)
    assert len(payloads) == 1
    return {"requests": _request_sequence(soda_cloud), "payload": _mask_run_varying_values(payloads[0])}


def _request_sequence(soda_cloud: MockSodaCloud) -> list[str]:
    # The path tells the file upload from a command; a command's type tells the commands apart.
    return [
        " ".join(
            part
            for part in (urlparse(request.url).path, (request.json or {}).get("type"))
            if isinstance(part, str) and part
        )
        for request in soda_cloud.requests
    ]


def _mask_run_varying_values(payload: dict) -> dict:
    masked: dict = copy.deepcopy(payload)
    for key in ("scanId", "scanStartTimestamp", "scanEndTimestamp"):
        if key in masked:
            masked[key] = MASK
    masked.pop("logs", None)
    return masked


def _to_json_text(sent: dict) -> str:
    # No sort_keys: the key order is part of what the engine emits.
    return json.dumps(sent, indent=2, ensure_ascii=False) + "\n"


def _load_or_update_snapshot(snapshot_path: Path, sent: dict) -> str:
    if updating_snapshots():
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(_to_json_text(sent), encoding="utf-8")
        pytest.skip(f"Updated {snapshot_path.name}; review the diff and rerun without {UPDATE_SNAPSHOTS_ENV_VAR}")
    if not snapshot_path.exists():
        pytest.fail(f"No snapshot at {snapshot_path}. Take it with {UPDATE_SNAPSHOTS_ENV_VAR}=1.", pytrace=False)
    return snapshot_path.read_text(encoding="utf-8")


def _assert_matches_snapshot(sent: dict, snapshot_path: Path = SNAPSHOT_PATH) -> None:
    expected: list[str] = _load_or_update_snapshot(snapshot_path, sent).splitlines()
    actual: list[str] = _to_json_text(sent).splitlines()
    if actual != expected:
        diff: str = "\n".join(difflib.unified_diff(expected, actual, fromfile="snapshot", tofile="actual", lineterm=""))
        pytest.fail(
            f"What the unscoped contract sends to Soda Cloud changed. If that is intended, update the snapshot with "
            f"{UPDATE_SNAPSHOTS_ENV_VAR}=1.\n{diff}",
            pytrace=False,
        )


def test_unscoped_contract_sends_what_it_sent_before(monkeypatch):
    sent: dict = _verify_unscoped_contract(monkeypatch)

    assert sent["payload"]["defaultDataSourceProperties"] == {"type": "duckdb"}
    _assert_matches_snapshot(sent)


FILTERED_RUNS = pytest.mark.parametrize(
    "snapshot_name, check_paths, check_selectors",
    [
        (
            "scan_results_payload_unscoped_check_paths.json",
            ["columns.amount.checks.invalid.strict", "checks.metric.query"],
            None,
        ),
        ("scan_results_payload_unscoped_check_selector.json", None, ["type=row_count"]),
    ],
    ids=["check-paths", "check-selector"],
)


@FILTERED_RUNS
def test_filtered_unscoped_contract_sends_what_it_sent_before(monkeypatch, snapshot_name, check_paths, check_selectors):
    sent: dict = _verify_unscoped_contract(
        monkeypatch, check_paths=check_paths, check_selectors=CheckSelector.parse_all(check_selectors)
    )

    assert any(check["outcome"] == "excluded" for check in sent["payload"]["checks"])
    _assert_matches_snapshot(sent, Path(__file__).parent / "snapshots" / snapshot_name)


def _without_location(check: dict) -> dict:
    return {key: value for key, value in check.items() if key != "location"}


def _snapshot_checks() -> list[dict]:
    return json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))["payload"]["checks"]


# Pins the NOT_EVALUATED outcome of core alone, so it drops the soda-scopes extension wherever that is installed.
@pytest.mark.usefixtures("without_scopes_extension")
def test_declared_scopes_leave_the_unscoped_checks_as_in_the_snapshot(monkeypatch):
    if updating_snapshots():
        pytest.skip(f"Updates nothing; rerun without {UPDATE_SNAPSHOTS_ENV_VAR}")
    session_result, payload = verify_contract(monkeypatch, scoped_contract_yaml())
    snapshot_identities: set[str] = {check["identities"]["vc1"] for check in _snapshot_checks()}
    unscoped_checks = [check for check in payload["checks"] if check["identities"]["vc1"] in snapshot_identities]
    scoped_checks = [check for check in payload["checks"] if check["identities"]["vc1"] not in snapshot_identities]

    # Every check in the snapshot, unchanged but for its line number.
    assert [_without_location(check) for check in unscoped_checks] == [
        _without_location(check) for check in _snapshot_checks()
    ]
    # The scoped copies get identities of their own, distinct from each other too.
    assert len({check["identities"]["vc1"] for check in scoped_checks}) == 3
    assert [(check["outcome"], check["source"]) for check in scoped_checks] == [("unevaluated", "soda-contract")] * 3
    assert session_result.number_of_checks_excluded == 0
