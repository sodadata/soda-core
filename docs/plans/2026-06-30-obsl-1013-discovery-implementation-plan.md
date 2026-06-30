# OBSL-1013 Discovery in soda-core — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship table discovery as first-party soda-core functionality — a `soda data-source discover` command that posts a DQN-only `sodaCoreInsertScanResults` payload (`version: "4"`) the BE's v4 `DiscoveryScanHandlerModule` accepts.

**Architecture:** A `DatasetIdentifier.from_object(...)` factory turns each discovered `FullyQualifiedObjectName` into a dialect-correct DQN using the dialect's prefix-index hooks; `DiscoveryRun.execute(...)` discovers within a scope, filters, and maps each object through that factory; a `discovery_payload` module wraps the DQNs in the Cloud envelope and posts via the generic `SodaCloud._execute_command`; and a `data-source discover` CLI command + handler wire it together. Discovery is NOT a `CheckCollectionImpl`.

**Tech Stack:** Python, argparse CLI, pydantic data-source models, pytest (`.venv/bin/pytest`), postgres test DB.

**Decisions (from design):**
- CLI shape: `soda data-source discover` (under the existing `data-source` resource).
- DQN construction: **per discovered object**, via `DatasetIdentifier.from_object`, using the dialect's `get_database_prefix_index()`/`get_schema_prefix_index()` hooks. This is the inverse of the existing `extract_database_from_prefix`/`extract_schema_from_prefix`, so it's consistent with how soda-core reads prefixes everywhere else. No hand-rolled `[database, schema]` and no per-dialect namespace override.
- Test strategy: self-contained structural snapshot — in-process flow + `MockSodaCloud`; assert exit OK, DQN-only `metadata`, the test table's DQN present/well-formed, `version == "4"`.

**Why per-object + dialect hooks (verified across all dialects):** the `MetadataTablesQuery` layer already normalizes each dialect into `FullyQualifiedObjectName(database_name, schema_name, …)` (postgres → `current_database()`+schema; BigQuery → project+dataset; oracle/db2/sparkdf → `None`+schema). Naive `[database_name, schema_name]` drop-None is **wrong for duckdb**, which populates a catalog in `database_name` but sets `get_database_prefix_index() == None` (DQN must be `[schema]`). Composing via the index hooks handles every case: postgres `[db, schema]`, duckdb `[schema]`, BigQuery `[project, dataset]`, oracle/db2/sparkdf `[schema]`.

**Flagged assumptions for the implementer:**
1. **Query scope.** The handler passes `prefixes=[]` to `discover_qualified_objects` (discover everything visible to the connection, matching v3's discover-all behavior); include/exclude + `__soda_temp` filtering narrow results. Per-dialect scope refinement (e.g. excluding postgres system schemas, BigQuery project/dataset scoping) is a follow-up and does not affect DQN correctness. The integration test scopes to the test schema (via the helper) for determinism.
2. **Envelope fields.** The payload mirrors the golden's non-volatile fields for BE DTO deserialization safety. If BE rejects, add the missing field — never drop `metadata`/`version`.

**Test run note:** `.venv/bin/pytest`, `TEST_DATASOURCE=postgres` for the integration task. Branch: `obsl-1013-discovery-ship-tablecolumn-discovery-in-soda-core`.

---

## File Structure

| File | Responsibility |
| -- | -- |
| `soda-core/src/soda_core/common/dataset_identifier.py` (modify) | Add `DatasetIdentifier.from_object(...)` factory |
| `soda-core/src/soda_core/discovery/__init__.py` (create) | Package marker |
| `soda-core/src/soda_core/discovery/discovery_run.py` (create) | `DiscoveryRun.execute(...) -> list[str]` |
| `soda-core/src/soda_core/discovery/discovery_payload.py` (create) | `build_discovery_payload(...)` + `send_discovery_results(...)` |
| `soda-core/src/soda_core/cli/handlers/data_source.py` (modify) | `handle_discover_data_source(...)` |
| `soda-core/src/soda_core/cli/cli.py` (modify) | `_setup_data_source_discover_command(...)` + register |
| `soda-tests/tests/unit/test_dataset_identifier_from_object.py` (create) | Unit: dialect-correct DQN composition |
| `soda-tests/tests/unit/test_discovery_run.py` (create) | Unit: filtering + mapping |
| `soda-tests/tests/unit/test_discovery_payload.py` (create) | Unit: payload shape |
| `soda-tests/tests/unit/test_cli_discover.py` (create) | Unit: CLI arg → handler mapping |
| `soda-tests/tests/integration/test_discovery.py` (create) | Integration snapshot: postgres + MockSodaCloud |

---

## Task 1: `DatasetIdentifier.from_object` — dialect-correct DQN from a discovered object

**Files:**
- Modify: `soda-core/src/soda_core/common/dataset_identifier.py`
- Test: `soda-tests/tests/unit/test_dataset_identifier_from_object.py`

- [ ] **Step 1: Write the failing test**

```python
# soda-tests/tests/unit/test_dataset_identifier_from_object.py
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.statements.table_types import FullyQualifiedTableName


class _FakeDialect:
    """Stands in for a SqlDialect: only the two prefix-index hooks are used."""

    def __init__(self, database_prefix_index, schema_prefix_index):
        self._db = database_prefix_index
        self._schema = schema_prefix_index

    def get_database_prefix_index(self):
        return self._db

    def get_schema_prefix_index(self):
        return self._schema


def test_from_object_postgres_like_includes_database():
    obj = FullyQualifiedTableName(database_name="soda", schema_name="public", table_name="customers")
    di = DatasetIdentifier.from_object("postgres", _FakeDialect(0, 1), obj)
    assert di.to_string() == "postgres/soda/public/customers"


def test_from_object_duckdb_like_drops_catalog():
    # duckdb populates database_name with a catalog but get_database_prefix_index() is None,
    # so the catalog must NOT appear in the DQN.
    obj = FullyQualifiedTableName(database_name="memory", schema_name="main", table_name="t")
    di = DatasetIdentifier.from_object("dd", _FakeDialect(None, 0), obj)
    assert di.to_string() == "dd/main/t"


def test_from_object_no_database_value():
    # oracle/db2/sparkdf: database_name is None -> schema-only DQN.
    obj = FullyQualifiedTableName(database_name=None, schema_name="HR", table_name="EMP")
    di = DatasetIdentifier.from_object("ora", _FakeDialect(0, 1), obj)
    assert di.to_string() == "ora/HR/EMP"
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest soda-tests/tests/unit/test_dataset_identifier_from_object.py -v`
Expected: FAIL — `AttributeError: type object 'DatasetIdentifier' has no attribute 'from_object'`

- [ ] **Step 3: Implement**

In `soda-core/src/soda_core/common/dataset_identifier.py`, add this classmethod to the `DatasetIdentifier` class (e.g. after `parse`). Add `from typing import TYPE_CHECKING` usage for the type hints if desired, but duck-typed args are fine — do NOT add a hard import of `SqlDialect`/`FullyQualifiedObjectName` at module top (avoid import cycles); reference them only in a `TYPE_CHECKING` block or leave them unannotated.

```python
    @classmethod
    def from_object(cls, data_source_name, sql_dialect, fully_qualified_object_name) -> "DatasetIdentifier":
        """Build a dialect-correct DQN from a discovered FullyQualifiedObjectName.

        Uses the dialect's prefix-index hooks (the inverse of
        extract_database_from_prefix / extract_schema_from_prefix): the database
        component is included only when the dialect has a database tier
        (get_database_prefix_index() is not None) and the object carries one;
        the schema component is appended when present. Database always precedes
        schema across all current dialects.
        """
        prefixes: list[str] = []
        if (
            sql_dialect.get_database_prefix_index() is not None
            and fully_qualified_object_name.database_name is not None
        ):
            prefixes.append(fully_qualified_object_name.database_name)
        if (
            sql_dialect.get_schema_prefix_index() is not None
            and fully_qualified_object_name.schema_name is not None
        ):
            prefixes.append(fully_qualified_object_name.schema_name)
        return cls(
            data_source_name=data_source_name,
            prefixes=prefixes,
            dataset_name=fully_qualified_object_name.get_object_name(),
        )
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest soda-tests/tests/unit/test_dataset_identifier_from_object.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add soda-core/src/soda_core/common/dataset_identifier.py soda-tests/tests/unit/test_dataset_identifier_from_object.py
git commit -m "feat(discovery): DatasetIdentifier.from_object builds dialect-correct DQNs" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: `DiscoveryRun` — discover, filter, map to DQNs

**Files:**
- Create: `soda-core/src/soda_core/discovery/__init__.py` (empty)
- Create: `soda-core/src/soda_core/discovery/discovery_run.py`
- Test: `soda-tests/tests/unit/test_discovery_run.py`

- [ ] **Step 1: Write the failing test**

```python
# soda-tests/tests/unit/test_discovery_run.py
from soda_core.common.statements.table_types import FullyQualifiedTableName
from soda_core.discovery.discovery_run import DiscoveryRun


class _FakeDialect:
    def get_database_prefix_index(self):
        return 0

    def get_schema_prefix_index(self):
        return 1


class _FakeDataSource:
    """Minimal DataSourceImpl stand-in: name, sql_dialect, discover_qualified_objects."""

    def __init__(self, object_names):
        self.name = "postgres"
        self.sql_dialect = _FakeDialect()
        self._object_names = object_names

    def discover_qualified_objects(self, prefixes, object_types=None):
        return [
            FullyQualifiedTableName(database_name="soda", schema_name="public", table_name=n)
            for n in self._object_names
        ]


def test_builds_dqns_and_filters_soda_temp():
    ds = _FakeDataSource(["customers", "orders", "__soda_temp_x"])
    assert DiscoveryRun.execute(ds, prefixes=[]) == [
        "postgres/soda/public/customers",
        "postgres/soda/public/orders",
    ]


def test_include_exclude_patterns():
    ds = _FakeDataSource(["customers", "orders", "cust_archive"])
    assert DiscoveryRun.execute(ds, prefixes=[], include=["cust%"]) == [
        "postgres/soda/public/customers",
        "postgres/soda/public/cust_archive",
    ]
    assert DiscoveryRun.execute(ds, prefixes=[], exclude=["cust%"]) == [
        "postgres/soda/public/orders",
    ]
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest soda-tests/tests/unit/test_discovery_run.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'soda_core.discovery'`

- [ ] **Step 3: Implement**

Create `soda-core/src/soda_core/discovery/__init__.py` (empty). Then `soda-core/src/soda_core/discovery/discovery_run.py`:

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
        objects = [o for o in objects if not o.get_object_name().lower().startswith(SODA_TEMP_PREFIX)]
        if include:
            objects = [o for o in objects if _matches_any(o.get_object_name(), include)]
        if exclude:
            objects = [o for o in objects if not _matches_any(o.get_object_name(), exclude)]
        return [
            DatasetIdentifier.from_object(data_source_impl.name, data_source_impl.sql_dialect, o).to_string()
            for o in objects
        ]
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest soda-tests/tests/unit/test_discovery_run.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add soda-core/src/soda_core/discovery/__init__.py soda-core/src/soda_core/discovery/discovery_run.py soda-tests/tests/unit/test_discovery_run.py
git commit -m "feat(discovery): DiscoveryRun discovers, filters, maps to DQNs" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Discovery payload builder + sender

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


def build_discovery_payload(dqns: list[str], data_source_name: str, scan_definition_name: str) -> dict:
    """DQN-only sodaCoreInsertScanResults body for v4 discovery. Mirrors the non-routing
    fields v3 emits for BE DTO deserialization safety."""
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

## Task 4: `handle_discover_data_source` handler

**Files:**
- Modify: `soda-core/src/soda_core/cli/handlers/data_source.py` (append; reuses existing imports: `ExitCode`, `soda_logger`, `Emoticons`, `DataSourceYamlSource`, `SodaCloud`, `SodaCloudYamlSource`, `Optional`)
- Verified by Task 6 (integration); no separate unit test (thin glue needing a real data source + cloud).

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

    try:
        # Scope: discover everything visible to the connection (v3 behaviour); include/exclude narrow it.
        dqns: list[str] = DiscoveryRun.execute(
            data_source_impl=data_source_impl,
            prefixes=[],
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
Expected: prints `ok`

- [ ] **Step 3: Commit**

```bash
git add soda-core/src/soda_core/cli/handlers/data_source.py
git commit -m "feat(discovery): handle_discover_data_source handler" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: CLI wiring + arg-mapping test

**Files:**
- Modify: `soda-core/src/soda_core/cli/cli.py`
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
Expected: FAIL — `discover` not a valid `data-source` command (SystemExit 2) or import error.

- [ ] **Step 3: Implement**

In `soda-core/src/soda_core/cli/cli.py`, add `handle_discover_data_source` to the existing data_source handlers import (the line `from soda_core.cli.handlers.data_source import handle_create_data_source, handle_test_data_source`).

Register the command in `_setup_data_source_resource` (lines 422-428):

```python
def _setup_data_source_resource(resource_parsers) -> None:
    data_source_parser = resource_parsers.add_parser("data-source", help="Data source commands")
    data_source_subparsers = data_source_parser.add_subparsers(dest="command", help="Data source commands")

    _setup_data_source_create_command(data_source_subparsers)
    _setup_data_source_test_command(data_source_subparsers)
    _setup_data_source_discover_command(data_source_subparsers)
```

Add the command setup right after `_setup_data_source_test_command` (after line 488):

```python
def _setup_data_source_discover_command(data_source_parsers) -> None:
    discover_parser = data_source_parsers.add_parser("discover", help="Discover datasets in a data source")
    discover_parser.add_argument("-ds", "--data-source", type=str, help="The data source configuration file.")
    discover_parser.add_argument(
        "--include", type=str, nargs="*", help="Dataset name patterns to include (SQL %% wildcard)."
    )
    discover_parser.add_argument(
        "--exclude", type=str, nargs="*", help="Dataset name patterns to exclude (SQL %% wildcard)."
    )
    discover_parser.add_argument("--scan-definition-name", type=str, help="Override the scan definition name.")
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

## Task 6: Integration snapshot (postgres + MockSodaCloud)

**Files:**
- Test: `soda-tests/tests/integration/test_discovery.py`

End-to-end against postgres: capture the posted payload via `MockSodaCloud`, assert DQN-only `metadata`, `version: "4"`, and the test table's well-formed DQN. This exercises the real `DatasetIdentifier.from_object` against a live postgres dialect (the `[db, schema]` path).

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

    # Scope discovery to the test schema for determinism (shared CI DB has many schemas).
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
    assert all(set(entry.keys()) == {"datasetQualifiedName"} for entry in posted["metadata"])
    expected_suffix = test_table.unique_name.lower()
    assert any(
        entry["datasetQualifiedName"].lower().endswith(expected_suffix) for entry in posted["metadata"]
    ), f"{test_table.unique_name} not found in {posted['metadata']}"
```

- [ ] **Step 2: Run against postgres**

Run: `TEST_DATASOURCE=postgres .venv/bin/pytest soda-tests/tests/integration/test_discovery.py -v`
Expected: PASS (1 passed). On failure, print `posted["metadata"]` and reconcile the DQN — but do NOT loosen the DQN-only / `version == "4"` checks.

- [ ] **Step 3: Commit**

```bash
git add soda-tests/tests/integration/test_discovery.py
git commit -m "test(discovery): integration snapshot of DQN-only v4 payload" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage (OBSL-1013):**
- `DiscoveryRun.execute(...)` → Task 2. ✓ (signature keeps the ticket's `prefixes` as scope)
- bulk `discover_qualified_objects` + include/exclude + `__soda_temp` + DQN via `DatasetIdentifier` → Tasks 1–2. ✓
- DQN-only payload + `version: "4"` → Task 3. ✓
- `soda data-source discover` CLI + `ExitCode` semantics → Tasks 4–5. ✓
- snapshot test vs postgres via `mock_soda_cloud` → Task 6. ✓
- documented limitation (hierarchy only, not columns) → header + DQN-only payload. ✓

**Placeholder scan:** none — every code step has complete code.

**Type/name consistency:** `DatasetIdentifier.from_object(data_source_name, sql_dialect, fully_qualified_object_name)`, `DiscoveryRun.execute(data_source_impl, prefixes, include, exclude) -> list[str]`, `build_discovery_payload(dqns, data_source_name, scan_definition_name)`, `send_discovery_results(soda_cloud, payload)`, `handle_discover_data_source(data_source_file_path, include, exclude, scan_definition_name, soda_cloud_file_path)` — consistent across tasks. ✓

**Open risks:** (1) query scope `prefixes=[]` discovers all visible schemas incl. system schemas on postgres — flagged as a follow-up refinement; does not affect DQN correctness. (2) envelope completeness vs the real BE — confirmed only at parity time, not here. Both flagged in the header.
