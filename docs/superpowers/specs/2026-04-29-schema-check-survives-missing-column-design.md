# Schema check survives missing-column SQL errors

- **Date:** 2026-04-29
- **Status:** Approved (design)
- **Owner:** Milan Lukac
- **Origin:** Bug report from Tyler Adkins (Slack thread `C08AAF7EEJD/p1777123377352949`)

## Background

Today, when a contract YAML defines a column-level check (e.g., `missing_count`) on a column that has been dropped from the table, scanning the contract aborts with an opaque SQL error:

```
An exception occurred: Could not execute aggregation query: column "billing_street" does not exist
```

A schema check defined in the same contract is **not evaluated**, even though it would have produced exactly the violation the user is looking for ("missing column `billing_street`"). The user is left guessing.

### Root cause

In `soda-core/src/soda_core/contracts/impl/contract_verification_impl.py`:

- The query execution loop at lines 642–644 has no exception handling. Any single query failure aborts the scan.
- `AggregationQuery.execute()` raises `SodaCoreException` (line 1813) when the underlying SQL fails.
- Schema check evaluation runs *after* the query loop (line 659), so it never gets a chance.

The schema check itself is already self-contained: it runs its own `SchemaQuery` against information_schema (`schema_check.py:273-303`) and `evaluate()` already handles a missing measurement gracefully (`schema_check.py:139-154`). The only thing standing in the way is the unhandled exception.

## Goal

When a column-level check's SQL query fails with `SodaCoreException`, the scan must continue so the schema check runs and reports the real violation.

## Non-goals

- Adding a new `ERROR` value to `CheckOutcome`.
- New per-check diagnostic fields, error tracking on `MeasurementValues`, or any new e2e capability. This is a fix.
- Pre-validating column references against live schema before building aggregation SQL.
- Enforcing that contracts always include a schema check.
- Reconciliation-specific UX for differentiating "missing column on source" vs "on target".

## Design

### Wrap the query execution loop

`contract_verification_impl.py:642-644`. Catch `SodaCoreException` only. Other exceptions still abort the scan — programming bugs and infrastructure failures must remain loud.

On catch: log at `ERROR` on the contract logger (which feeds `ContractVerificationResult.log_records`, surfaced via `get_errors()` at lines 536–537), and continue the loop. The schema query and other queries still execute.

That is the entire change. No other files are modified.

### Why this is sufficient

- The schema check is self-contained (`schema_check.py:273-303`) and runs from its own `SchemaQuery`. Once the loop no longer aborts, it evaluates and reports the missing column as `FAILED`.
- Column-level checks (missing/invalid/duplicate/aggregate/metric) already short-circuit to `NOT_EVALUATED` when `measurement_values.get_value()` returns `None`. No change to their `evaluate()` is needed.
- The SQL error is already surfaced once via the contract-level error log. A user investigating sees the schema-check `FAILED` outcome (the actionable one) and the raw SQL error in the errors section.

### Reporting

**CLI output** for the canonical scenario (schema check + `missing_count(billing_street)` on a table missing that column):

```
schema                                            FAILED
  Missing column 'billing_street' in actual table

missing_count(billing_street) < 1                 NOT_EVALUATED

Errors:
  - Aggregation query failed: column "billing_street" does not exist.
```

**Exit code.** `soda contract verify` continues to exit non-zero because the schema check is `FAILED`. *Edge case:* a contract with no schema check where every column-level check on the missing column degrades to `NOT_EVALUATED` would exit clean. Known limitation; contract authors should always include a schema check.

**Known limitation — query batching.** Missing/invalid metrics across multiple columns are batched into a single `AggregationQuery` (built in `_build_queries()` around `contract_verification_impl.py:584-597`). When one column reference in that batch fails, every metric in the same batch loses its measurement and the corresponding checks all degrade to `NOT_EVALUATED` — including checks on healthy columns that happened to share the batch. The schema check still runs and reports the real cause, but per-column resilience for column-level checks is out of scope here. Tracked as a follow-up under the larger "pre-validate column references" effort (Option B from the analysis).

**Soda Cloud impact.** None. No wire-format change.

### Extension impact

| Component | Affected? | Action |
|---|---|---|
| soda-core built-in checks (missing/invalid/duplicate/aggregate/metric) | Inherits fix | Already short-circuit to `NOT_EVALUATED` on `None` measurements; no change |
| soda-reconciliation | Inherits fix | Aggregate metrics flow through the same `AggregationQuery` path; gets the protection for free |
| soda-groupby | Already mitigated | `GroupbyMetricQuery.execute()` (groupby_check.py:222-226) already wraps `execute_query` in try/except; leave alone |
| soda-oracle, soda-dremio | Inherits fix | Don't override execution; core-level fix protects them |
| soda/check_extensions/* | Inherits fix | Use core `AggregationQuery` |
| soda-migration, soda-autopilot | Not in verify flow | No change |

## Test plan

Tests live under `soda-tests/tests/`. Run on at least postgres + duckdb (the two fastest data sources in the matrix).

1. **Happy-path regression.** Existing schema-check tests still pass — no behavior change when columns exist.
2. **Missing column with schema check.** Contract has schema check + `missing_count` on `billing_street`; table doesn't have that column. Assert: schema check `FAILED` with "missing column" message; `missing_count` check `NOT_EVALUATED`; an ERROR-level entry in `ContractVerificationResult.log_records` mentioning `billing_street`.
3. **Missing column without schema check.** Same as (2) but no schema check. Assert: `missing_count` is `NOT_EVALUATED`; contract has the error log; no crash.
4. **Multi-column contract scan completes with mixed missing/existing columns.** Schema check still runs and reports the missing column. All column-level missing checks degrade to `NOT_EVALUATED` due to query batching (see Known Limitation above) — the test pins the actual behavior so future readers don't expect per-column resilience.
5. **Non-`SodaCoreException` still aborts.** Inject a different exception type into a query's `execute()` and assert the scan still raises (programming bugs stay loud).

## Open questions

None at design time. Implementation will surface the exact module path for new tests and the precise check-result construction site for `diagnostic_message` population.

## References

- Slack: `C08AAF7EEJD/p1777123377352949` (parent), `CAXRR8SS3/p1776811607494269` (original report)
- Code: `soda-core/src/soda_core/contracts/impl/contract_verification_impl.py:642-644` (loop), `:1813` (`AggregationQuery.execute` raise site)
- Code: `soda-core/src/soda_core/contracts/impl/check_types/schema_check.py:139-154` (graceful None handling), `:273-303` (self-contained schema query)
- Code: `soda-core/src/soda_core/contracts/api/contract_verification.py:510, :536-537` (log_records / get_errors)
