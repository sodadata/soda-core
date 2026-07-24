# SQL Server version-based capability derivation — design

**Date:** 2026-07-24
**Branch:** `obsl-1036-dialect-groundwork-all-datasources` (PR #2787)
**Status:** approved

## Problem

The current `APPROX_PERCENTILE_DISC` version guard (commit `055176ce`) inverts the
desired responsibility split: `SqlServerDataSourceImpl` probes the engine
(`SERVERPROPERTY('ProductMajorVersion')` / `EngineEdition`), derives a capability
boolean in `_engine_supports_approx_percentile_disc`, and injects that pre-chewed
boolean into the dialect via `set_approx_percentile_disc_supported()`. The dialect
never sees the version, so every future version-dependent capability needs its own
probe/setter pair in the connector.

## Goal

Mirror the Oracle pattern from soda-extensions #2795: the connection detects raw
server facts, the data source syncs them onto the dialect, and the dialect derives
capabilities itself. The connector computes no capabilities; the dialect touches no
connection.

## Decisions (with rationale)

1. **The dialect receives two raw facts** — `server_major_version` and
   `engine_edition` — not a derived boolean and not a normalized version. A version
   number alone cannot decide this capability: Azure SQL Database (EngineEdition 5)
   and Managed Instance (EngineEdition 8) report legacy `ProductMajorVersion` 12 but
   support `APPROX_PERCENTILE_DISC`; on-prem needs 2022+ (major ≥ 16).
2. **Detection is eager, at connection open, in the connection class.**
   `server_major_version` comes free from the pyodbc login handshake
   (`connection.getinfo(pyodbc.SQL_DBMS_VER)`); `engine_edition` costs one
   `SELECT CAST(SERVERPROPERTY('EngineEdition') AS INT)` query. Lazy detection was
   rejected: reading the connection during SQL generation breaks snapshot replay
   (see the explicit warning in `oracle_data_source.py`).
3. **Facts are synced onto the dialect after creation (Oracle-style setter/sync).**
   Constructor injection was explored and rejected: the base `DataSourceImpl.__init__`
   creates the dialect before any connection exists, and contract parsing /
   `only_validate_without_execute` read `sql_dialect` without ever opening a
   connection — so constructor injection would force replacing the dialect instance
   at connection open, which we don't want. A lazy `server_info_provider` callable
   was also explored and rejected as over-engineered.
4. **SQL-Server-local pattern.** No generalization into the `SqlDialect` base in
   soda-core. Oracle and SQL Server are two local precedents; unify in core when a
   third dialect needs it (rule of three).

## Design

### 1. Detection — `SqlServerDataSourceConnection`

After the pyodbc connection is created in `_create_connection`:

- `self.server_major_version: Optional[int]` — parsed from
  `connection.getinfo(pyodbc.SQL_DBMS_VER)` (e.g. `"15.00.4123"` → `15`) via a
  static `_parse_server_major_version` helper (same shape as Oracle's).
- `self.engine_edition: Optional[int]` — one `SERVERPROPERTY('EngineEdition')` query.

Any detection failure logs a warning and leaves the attribute `None`; detection never
fails an otherwise healthy connect. Fabric/Synapse connections inherit this; the one
extra query at their connect is harmless (their dialects pin the capability, so a
`None` there is inert).

### 2. Sync — `SqlServerDataSourceImpl`

The current `open_connection` override (probe + boolean push) is replaced by
`_sync_dialect_server_info()`, called from:

- `open_connection()`, after `super().open_connection()`, and
- `__init__` when a live connection is passed in (the
  `create_copy_with_different_connection` path — Oracle has the same two call sites).

It copies both attributes onto the dialect, guarded by
`isinstance(conn, SqlServerDataSourceConnection)`. This guard is load-bearing, not
defensive: in snapshot replay the connection is a `SnapshotDataSourceConnection`
lazy wrapper (`soda-tests/src/helpers/snapshot_connection.py`) whose `__getattr__`
opens a real connection on attribute access — duck-typing the attributes would break
replay (same rationale as documented on Oracle's `_sync_dialect_server_version`).
Skipping the sync there is also correct: replay has no live server facts, and the
dialect's `None` default is what recorded snapshots expect.

The dialect side needs no runtime guard — every `_create_sql_dialect` in the
hierarchy returns a `SqlServerSqlDialect` subclass — but the sync uses
`assert isinstance(self.sql_dialect, SqlServerSqlDialect)` purely to narrow the
declared `SqlDialect` type for the attribute assignment. The current
`type(...) is SqlServerSqlDialect` exact-type guard disappears: syncing facts onto
Fabric/Synapse dialects is inert because they override the capability method.

### 3. Derivation — `SqlServerSqlDialect`

`__init__` sets `self.server_major_version: Optional[int] = None` and
`self.engine_edition: Optional[int] = None`. The old seam
(`_approx_percentile_disc_supported`, `set_approx_percentile_disc_supported`,
`_probe_approx_percentile_disc_support`, `_engine_supports_approx_percentile_disc`)
is deleted. The decision matrix moves into the dialect:

```python
def supports_percentile_within_group(self) -> bool:
    if self.server_major_version is None and self.engine_edition is None:
        return True  # no live server facts (offline rendering, unit tests,
                     # snapshot replay): assume newest engine, like Oracle
    return (
        self.server_major_version is not None
        and self.server_major_version >= SQLSERVER_2022_MAJOR_VERSION
    ) or self.engine_edition in (
        AZURE_SQL_DATABASE_ENGINE_EDITION,      # 5
        AZURE_SQL_MANAGED_INSTANCE_ENGINE_EDITION,  # 8
    )
```

Named module constants replace the magic numbers (`SQLSERVER_2022_MAJOR_VERSION = 16`,
editions 5 and 8).

One deliberate semantic change: the old code distinguished a *failed probe*
(→ assume support) from a *live server reporting NULL for both properties*
(→ `False`, per the old matrix test `f(None, None) is False`). With facts stored as
attributes, "offline" and "live but unknown" are indistinguishable, so `(None, None)`
uniformly means assume support. The NULL/NULL-from-a-live-server case is not known to
occur in practice; assume-support merely restores pre-guard v3 behavior for it (the
real query surfaces any error).

### 4. Fabric / Synapse

Pins unchanged: Fabric overrides `supports_percentile_within_group()` → `True`,
Synapse → `False`. The Fabric test that currently calls
`set_approx_percentile_disc_supported(False)` changes to setting the version
attributes to a non-supporting combination and asserting the pin still wins.

## Error handling

- Detection failures: warning + `None`, never a failed connect (matches Oracle and
  the current probe's fallback).
- `None` facts at capability time: assume support — the real query surfaces any
  engine error, which is the pre-guard v3 behavior.

## Testing

- **Dialect unit tests** (port of the existing matrix test, moved from the data
  source to the dialect): set the two attributes, assert
  `supports_percentile_within_group()` — covering 2022+ (16/17), 2019 (15),
  Azure SQL DB (12/5), Managed Instance (12/8), Express (15/4), and the
  unknown-engine default — note the flip: `(None, None)` now asserts `True`
  (assume support), where the old matrix asserted `False` (see the semantic-change
  note above).
- **Connection unit test**: `_parse_server_major_version` on `"15.00.4123"`,
  `"12.00.2000"`, garbage, `None`.
- **Fabric/Synapse pin tests**: pins win regardless of synced facts.
- No integration-test changes expected: live behavior is identical.

## Out of scope

- Generalizing a version seam into `SqlDialect` (soda-core base).
- Migrating Oracle onto any shared mechanism.
- Any change to when connections open, or to dialect creation order in
  `DataSourceImpl`.
