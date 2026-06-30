# OBSL-1013 Discovery in soda-core — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship table discovery as first-party soda-core functionality — a `soda data-source discover` command that posts a DQN-only `sodaCoreInsertScanResults` payload (`version: "4"`) the BE's v4 `DiscoveryScanHandlerModule` accepts.

**Architecture:** A pure `DiscoveryRun.execute(data_source_impl, prefixes, include, exclude)` that bulk-discovers objects, filters them, and returns DQNs; a `discovery_payload` module that wraps DQNs in the Cloud envelope and posts via the generic `SodaCloud._execute_command`; and a `data-source discover` CLI command + handler wiring them together. Discovery is NOT a `CheckCollectionImpl`.

**Tech Stack:** Python, argparse CLI, pydantic data-source models, pytest (`.venv/bin/pytest`), postgres test DB.

**Decisions (from design):**
- CLI shape: `soda data-source discover` (under the existing `data-source` resource).
- Test strategy: self-contained structural snapshot — in-process flow + `MockSodaCloud`; assert exit OK, DQN-only `metadata`, the test table's DQN present/well-formed, `version == "4"`. The OBSL-1026 golden is a reference, not a byte-equality target (its DQN hash/`sourceOwner`/`definitionName` legitimately differ).

**v4 payload shape (from the corrected OBSL-1026 golden):** `metadata` is DQN-only — `[{"datasetQualifiedName": "<dqn>"}]`, no schema/row_count. `version` is `"4"`.

**Assumptions flagged for the implementer:**
1. **Prefix derivation.** No `get_default_prefixes()` exists on `DataSourceImpl`. For the postgres/duckdb launch tier, discovery scope is `prefixes = [database, schema]` (matching the golden DQN `postgres/soda/dev_mvds/<table>` → db=`soda`, schema=`dev_mvds`). The handler reads them from `data_source_impl.data_source_model.connection_properties.to_connection_kwargs()`. If a dialect orders prefixes differently, `extract_schema_from_prefix`/`extract_database_from_prefix` already encode the indices — keep `[database, schema]` for the launch tier.
2. **Envelope fields.** The payload mirrors the golden's non-volatile fields for BE DTO deserialization safety (`type`, `version`, `scanType`, `definitionName`, `defaultDataSource`, plus empty `checks`/`metrics`/`profiling`/`automatedMonitoringChecks`). If BE rejects, add the missing field — do not remove `metadata`/`version`.

**Test run note:** all pytest commands use `.venv/bin/pytest` and `TEST_DATASOURCE=postgres` for the integration task. The branch is `obsl-1013-discovery-ship-tablecolumn-discovery-in-soda-core`.

---

## File Structure

| File | Responsibility |
| -- | -- |
| `soda-core/src/soda_core/discovery/__init__.py` (create) | Package marker |
| `soda-core/src/soda_core/discovery/discovery_run.py` (create) | `DiscoveryRun.execute(...) -> list[str]` (DQNs): discover → filter → build DQNs |
| `soda-core/src/soda_core/discovery/discovery_payload.py` (create) | `build_discovery_payload(...)` + `send_discovery_results(...)` |
| `soda-core/src/soda_core/cli/handlers/data_source.py` (modify) | `handle_discover_data_source(...)` |
| `soda-core/src/soda_core/cli/cli.py` (modify) | `_setup_data_source_discover_command(...)` + register it |
| `soda-tests/tests/unit/test_discovery_run.py` (create) | Unit: filtering + DQN building |
| `soda-tests/tests/unit/test_discovery_payload.py` (create) | Unit: payload shape |
| `soda-tests/tests/unit/test_cli_discover.py` (create) | Unit: CLI arg → handler mapping |
| `soda-tests/tests/integration/test_discovery.py` (create) | Integration snapshot: postgres + MockSodaCloud |

---

## Task 1: `DiscoveryRun` — discover, filter, build DQNs

**Files:**
- Create: `soda-core/src/soda_core/discovery/__init__.py` (empty)
- Create: `soda-core/src/soda_core/discovery/discovery_run.py`
- Test: `soda-tests/tests/unit/test_discovery_run.py`

- [ ] **Step 1: Write the failing test**

```python
# soda-tests/tests/unit/test_discovery_run.py
from soda_core.common.statements.table_types import FullyQualifiedTableName
from soda_core.discovery.discovery_run import DiscoveryRun


class _FakeDataSource:
    """Minimal stand-in for DataSourceImpl for pure DiscoveryRun unit tests."""

    def __init__(self, name, object_names):
        self.name = name
        self._object_names = object_names

    def discover_qualified_objects(self, prefixes, object_types=None):
        return [
            FullyQualifiedTableName(database_name=prefixes[0], schema_name=prefixes[1], table_name=n)
            for n in self._object_names
        ]


def test_builds_dqns_and_filters_soda_temp():
    ds = _FakeDataSource("postgres", ["customers", "orders", "__soda_temp_x"])
    dqns = DiscoveryRun.execute(ds, prefixes=["soda", "public"])
    assert dqns == [
        "postgres/soda/public/customers",
        "postgres/soda/public/orders",
    ]


def test_include_exclude_patterns():
    ds = _FakeDataSource("postgres", ["customers", "orders", "cust_archive"])
    assert DiscoveryRun.execute(ds, prefixes=["soda", "public"], include=["cust%"]) == [
        "postgres/soda/public/customers",
        "postgres/soda/public/cust_archive",
    ]
    assert DiscoveryRun.execute(ds, prefixes=["soda", "public"], exclude=["cust%"]) == [
        "postgres/soda/public/orders",
    ]
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest soda-tests/tests/unit/test_discovery_run.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'soda_core.discovery'`

- [ ] **Step 3: Implement**

Create `soda-core/src/soda_core/discovery/__init__.py` (empty file). Then create `soda-core/src/soda_core/discovery/discovery_run.py`:

```python
# soda-core/src/soda_core/discovery/discovery_run.py
from __future__ import annotations

import fnmatch
from typing import Optional

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.dataset_identifier import DatasetIdentifier

SODA_TEMP_PREFIX = "__soda_temp"


def _matches_any(name: str, patterns: list[str]) -> bool:
    # v3 discovery uses SQL-style % wildcards; translate to fnmatch's * for matching.
    return any(fnmatch.fnmatch(name.lower(), pattern.replace("%", "*").lower()) for pattern in patterns)


class DiscoveryRun:
    """Discovers dataset names for a data source and returns their DQNs. Not a CheckCollectionImpl."""

    @staticmethod
    def execute(
        data_source_impl: DataSourceImpl,
        prefixes: list[str],
        include: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
    ) -> list[str]:
        objects = data_source_impl.discover_qualified_objects(prefixes=prefixes)
        names = [obj.get_object_name() for obj in objects]
        names = [n for n in names if not n.lower().startswith(SODA_TEMP_PREFIX)]
        if include:
            names = [n for n in names if _matches_any(n, include)]
        if exclude:
            names = [n for n in names if not _matches_any(n, exclude)]
        return [
            DatasetIdentifier(
                data_source_name=data_source_impl.name,
                prefixes=prefixes,
                dataset_name=name,
            ).to_string()
            for name in names
        ]
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest soda-tests/tests/unit/test_discovery_run.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add soda-core/src/soda_core/discovery/__init__.py soda-core/src/soda_core/discovery/discovery_run.py soda-tests/tests/unit/test_discovery_run.py
git commit -m "feat(discovery): DiscoveryRun builds filtered DQNs" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Discovery payload builder + sender

**Files:**
- Create: `soda-core/src/soda_core/discovery/discovery_payload.py`
- Test: `soda-tests/tests/unit/test_discovery_payload.py`

- [ ] **Step 1: Write the failing test**

```python
# soda-tests/tests/unit/test_discovery_payload.py
from soda_core.discovery.discovery_payload import build_discovery_payload


def test_payload_is_dqn_only_v4():
    payload = build_discovery_payload(
        dqns=["postgres/soda/public/customers", "postgres/soda/public/orders"],
        data_source_name="postgres",
        scan_definition_name="postgres_schema_discovery_scan",
    )
    assert payload["type"] == "sodaCoreInsertScanResults"
    assert payload["version"] == "4"
    assert payload["definitionName"] == "postgres_schema_discovery_scan"
    assert payload["defaultDataSource"] == "postgres"
    assert payload["metadata"] == [
        {"datasetQualifiedName": "postgres/soda/public/customers"},
        {"datasetQualifiedName": "postgres/soda/public/orders"},
    ]
    # DQN-only: no schema/row_count leak into metadata entries
    assert all(set(entry.keys()) == {"datasetQualifiedName"} for entry in payload["metadata"])
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest soda-tests/tests/unit/test_discovery_payload.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'soda_core.discovery.discovery_payload'`

- [ ] **Step 3: Implement**

```python
# soda-core/src/soda_core/discovery/discovery_payload.py
from __future__ import annotations

from typing import Optional

from requests import Response

from soda_core.common.soda_cloud import SodaCloud


def build_discovery_payload(
    dqns: list[str],
    data_source_name: str,
    scan_definition_name: str,
) -> dict:
    """Build the DQN-only sodaCoreInsertScanResults body for v4 discovery.
    Mirrors the non-routing fields v3 emits for BE DTO deserialization safety."""
    return {
        "type": "sodaCoreInsertScanResults",
        "version": "4",
        "scanType": None,
        "definitionName": scan_definition_name,
        "defaultDataSource": data_source_name,
        "metadata": [{"datasetQualifiedName": dqn} for dqn in dqns],
        "checks": [],
        "metrics": [],
        "profiling": [],
        "automatedMonitoringChecks": [],
    }


def send_discovery_results(soda_cloud: SodaCloud, payload: dict) -> Optional[Response]:
    return soda_cloud._execute_command(command_json_dict=payload, request_log_name="send_discovery_results")
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest soda-tests/tests/unit/test_discovery_payload.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add soda-core/src/soda_core/discovery/discovery_payload.py soda-tests/tests/unit/test_discovery_payload.py
git commit -m "feat(discovery): DQN-only v4 payload builder + sender" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: `handle_discover_data_source` handler

**Files:**
- Modify: `soda-core/src/soda_core/cli/handlers/data_source.py` (append a new handler function; imports already present: `ExitCode`, `soda_logger`, `Emoticons`, `DataSourceYamlSource`, `SodaCloud`, `SodaCloudYamlSource`)
- Test: covered by the integration task (Task 5); no separate unit test (handler is thin glue and needs a real data source + cloud).

- [ ] **Step 1: Implement the handler**

Append to `soda-core/src/soda_core/cli/handlers/data_source.py`:

```python
def handle_discover_data_source(
    data_source_file_path: str,
    include: Optional[list[str]] = None,
    exclude: Optional[list[str]] = None,
    scan_definition_name: Optional[str] = None,
    soda_cloud_file_path: Optional[str] = None,
) -> ExitCode:
    from soda_core.common.data_source_impl import DataSourceImpl
    from soda_core.discovery.discovery_payload import build_discovery_payload, send_discovery_results
    from soda_core.discovery.discovery_run import DiscoveryRun

    soda_logger.info(f"Discovering datasets for data source configuration file {data_source_file_path}")
    data_source_impl: Optional[DataSourceImpl] = DataSourceImpl.from_yaml_source(
        DataSourceYamlSource.from_file_path(data_source_file_path)
    )
    if data_source_impl is None:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Data source could not be created. See logs above (or -v).")
        return ExitCode.LOG_ERRORS

    if not soda_cloud_file_path:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Discovery requires a Soda Cloud configuration (-sc).")
        return ExitCode.LOG_ERRORS
    soda_cloud: Optional[SodaCloud] = SodaCloud.from_yaml_source(
        SodaCloudYamlSource.from_file_path(soda_cloud_file_path),
        provided_variable_values=None,
    )
    if soda_cloud is None:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Soda Cloud configuration could not be parsed.")
        return ExitCode.LOG_ERRORS

    # Prefix derivation for the launch tier (postgres/duckdb): [database, schema].
    conn_kwargs: dict = data_source_impl.data_source_model.connection_properties.to_connection_kwargs()
    database: Optional[str] = conn_kwargs.get("database")
    schema: Optional[str] = conn_kwargs.get("schema") or "public"
    prefixes: list[str] = [p for p in [database, schema] if p is not None]

    try:
        dqns: list[str] = DiscoveryRun.execute(
            data_source_impl=data_source_impl,
            prefixes=prefixes,
            include=include,
            exclude=exclude,
        )
    except Exception as exc:
        soda_logger.exception(f"Discovery query failed: {exc}")
        return ExitCode.LOG_ERRORS

    resolved_scan_definition_name: str = scan_definition_name or f"{data_source_impl.name}_schema_discovery_scan"
    payload: dict = build_discovery_payload(
        dqns=dqns,
        data_source_name=data_source_impl.name,
        scan_definition_name=resolved_scan_definition_name,
    )
    response = send_discovery_results(soda_cloud, payload)
    if response is None or not response.ok:
        soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Discovery results were not accepted by Soda Cloud.")
        return ExitCode.RESULTS_NOT_SENT_TO_CLOUD

    soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} Discovered {len(dqns)} datasets and sent results to Soda Cloud.")
    return ExitCode.OK
```

- [ ] **Step 2: Sanity import check**

Run: `.venv/bin/python -c "from soda_core.cli.handlers.data_source import handle_discover_data_source; print('ok')"`
Expected: prints `ok` (no import errors)

- [ ] **Step 3: Commit**

```bash
git add soda-core/src/soda_core/cli/handlers/data_source.py
git commit -m "feat(discovery): handle_discover_data_source handler" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: CLI wiring + arg-mapping test

**Files:**
- Modify: `soda-core/src/soda_core/cli/cli.py` (add `_setup_data_source_discover_command`, call it in `_setup_data_source_resource`, import the handler)
- Test: `soda-tests/tests/unit/test_cli_discover.py`

- [ ] **Step 1: Write the failing test**

```python
# soda-tests/tests/unit/test_cli_discover.py
import sys
from unittest.mock import patch

import pytest
from soda_core.cli.cli import create_cli_parser
from soda_core.cli.exit_codes import ExitCode


@patch("soda_core.cli.cli.handle_discover_data_source")
def test_cli_arg_mapping_for_data_source_discover(mock_handler):
    mock_handler.return_value = ExitCode.OK
    sys.argv = [
        "soda", "data-source", "discover",
        "-ds", "ds.yaml",
        "--include", "cust%",
        "--exclude", "tmp%",
        "--scan-definition-name", "my_scan",
        "-sc", "cloud.yaml",
    ]
    args = create_cli_parser().parse_args()
    with pytest.raises(SystemExit) as e:
        args.handler_func(args)
    assert e.value.code == ExitCode.OK
    mock_handler.assert_called_once_with(
        "ds.yaml", ["cust%"], ["tmp%"], "my_scan", soda_cloud_file_path="cloud.yaml"
    )
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest soda-tests/tests/unit/test_cli_discover.py -v`
Expected: FAIL — `discover` is not a valid `data-source` command (argparse error / SystemExit 2), or `handle_discover_data_source` import error.

- [ ] **Step 3: Implement the CLI wiring**

In `soda-core/src/soda_core/cli/cli.py`, add the handler to the existing handlers import (find the line importing from `soda_core.cli.handlers.data_source`, e.g. `from soda_core.cli.handlers.data_source import handle_create_data_source, handle_test_data_source` and add `handle_discover_data_source`).

Register the command in `_setup_data_source_resource` (currently lines 422-428):

```python
def _setup_data_source_resource(resource_parsers) -> None:
    data_source_parser = resource_parsers.add_parser("data-source", help="Data source commands")
    data_source_subparsers = data_source_parser.add_subparsers(dest="command", help="Data source commands")

    _setup_data_source_create_command(data_source_subparsers)
    _setup_data_source_test_command(data_source_subparsers)
    _setup_data_source_discover_command(data_source_subparsers)
```

Add the new command setup function (place it right after `_setup_data_source_test_command`, i.e. after line 488):

```python
def _setup_data_source_discover_command(data_source_parsers) -> None:
    discover_parser = data_source_parsers.add_parser("discover", help="Discover datasets in a data source")
    discover_parser.add_argument(
        "-ds", "--data-source", type=str, help="The data source configuration file."
    )
    discover_parser.add_argument(
        "--include", type=str, nargs="*", help="Dataset name patterns to include (SQL %% wildcard)."
    )
    discover_parser.add_argument(
        "--exclude", type=str, nargs="*", help="Dataset name patterns to exclude (SQL %% wildcard)."
    )
    discover_parser.add_argument(
        "--scan-definition-name", type=str, help="Override the scan definition name."
    )
    discover_parser.add_argument("-sc", "--soda-cloud", type=str, help=CLOUD_CONFIG_PATH_HELP)
    discover_parser.add_argument(
        "-v", "--verbose", const=True, action="store_const", default=False,
        help="Show more detailed logs on the console.",
    )

    def handle(args):
        exit_code = handle_discover_data_source(
            args.data_source,
            args.include,
            args.exclude,
            args.scan_definition_name,
            soda_cloud_file_path=args.soda_cloud,
        )
        exit_with_code(exit_code)

    discover_parser.set_defaults(handler_func=handle)
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest soda-tests/tests/unit/test_cli_discover.py -v`
Expected: PASS (1 passed)

- [ ] **Step 5: Commit**

```bash
git add soda-core/src/soda_core/cli/cli.py soda-tests/tests/unit/test_cli_discover.py
git commit -m "feat(discovery): soda data-source discover CLI command" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Integration snapshot test (postgres + MockSodaCloud)

**Files:**
- Test: `soda-tests/tests/integration/test_discovery.py`

This is the structural-snapshot test (the design's chosen parity strategy). It runs the real discovery flow against postgres, captures the posted payload via `MockSodaCloud`, and asserts the DQN-only shape + `version: "4"` + the test table's DQN.

- [ ] **Step 1: Write the test**

```python
# soda-tests/tests/integration/test_discovery.py
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification
from soda_core.discovery.discovery_payload import build_discovery_payload, send_discovery_results
from soda_core.discovery.discovery_run import DiscoveryRun

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("discovery")
    .column_varchar("id")
    .column_integer("age")
    .rows(rows=[("1", 30), ("2", 40)])
    .build()
)


def test_discovery_posts_dqn_only_v4_payload(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock(
        [MockResponse(status_code=200, json_object={"scanId": "discovery_scan"})]
    )

    prefixes = data_source_test_helper._create_dataset_prefix()
    dqns = DiscoveryRun.execute(data_source_test_helper.data_source_impl, prefixes=prefixes)

    payload = build_discovery_payload(
        dqns=dqns,
        data_source_name=data_source_test_helper.data_source_impl.name,
        scan_definition_name=f"{data_source_test_helper.data_source_impl.name}_schema_discovery_scan",
    )
    send_discovery_results(data_source_test_helper.soda_cloud, payload)

    posted = data_source_test_helper.soda_cloud.requests[0].json
    assert posted["type"] == "sodaCoreInsertScanResults"
    assert posted["version"] == "4"
    # DQN-only metadata: every entry has exactly datasetQualifiedName
    assert all(set(entry.keys()) == {"datasetQualifiedName"} for entry in posted["metadata"])
    # the discovered test table is present, as a well-formed DQN ending in the table name
    expected_suffix = test_table.unique_name.lower()
    assert any(
        entry["datasetQualifiedName"].lower().endswith(expected_suffix) for entry in posted["metadata"]
    ), f"{test_table.unique_name} not found in {posted['metadata']}"
```

- [ ] **Step 2: Run against postgres**

Run: `TEST_DATASOURCE=postgres .venv/bin/pytest soda-tests/tests/integration/test_discovery.py -v`
Expected: PASS (1 passed). If `_create_dataset_prefix()` or `unique_name` differs, inspect `posted["metadata"]` in the failure and adjust the assertion to the real DQN — but do NOT loosen the DQN-only / version checks.

- [ ] **Step 3: Commit**

```bash
git add soda-tests/tests/integration/test_discovery.py
git commit -m "test(discovery): integration snapshot of DQN-only v4 payload" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage (OBSL-1013 deliverables):**
- `discovery_run.py` `DiscoveryRun.execute(data_source_impl, prefixes, include, exclude)` → Task 1. ✓
- bulk `discover_qualified_objects` + include/exclude + `__soda_temp` filter + DQN via `DatasetIdentifier` → Task 1. ✓
- `discovery_payload.py` envelope + `metadata` + `version: "4"` → Task 2. ✓
- `soda data-source discover` CLI with `--include/--exclude/--scan-definition-name`, `ExitCode` semantics → Tasks 3–4. ✓
- snapshot test vs postgres via `mock_soda_cloud` → Task 5. ✓
- CLI test → Task 4 (arg-mapping; per the design's self-contained-snapshot decision, not a true subprocess). ✓
- DoD "posts a payload accepted by BE v4 handler" → structurally satisfied (DQN-only + version 4); BE acceptance verified at integration/parity time. ✓
- Documented limitation (hierarchy only, not columns) → captured in this plan header and the payload (DQN-only). Add to OBSL-1011 docs (out of scope here).

**Placeholder scan:** none — every code step has complete code.

**Type/name consistency:** `DiscoveryRun.execute(data_source_impl, prefixes, include, exclude) -> list[str]`, `build_discovery_payload(dqns, data_source_name, scan_definition_name)`, `send_discovery_results(soda_cloud, payload)`, `handle_discover_data_source(data_source_file_path, include, exclude, scan_definition_name, soda_cloud_file_path)` — used identically across tasks. ✓

**Open risk:** the two flagged assumptions (prefix derivation, envelope fields). Task 5 exercises prefix derivation against real postgres; if the DQN doesn't match the test table, the failure is visible and the fix is local. Envelope completeness is only truly confirmed against the BE (parity ticket), not in this plan.
