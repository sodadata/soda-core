# Schema check survives missing-column SQL errors — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop a column-level check's failed aggregation SQL from aborting the contract scan, so the schema check still evaluates and reports "missing column" cleanly.

**Architecture:** Wrap the query execution loop in `ContractImpl.verify()` with a narrow `try/except SodaCoreException` that logs the underlying SQL error to the contract logger and continues to the next query. The schema check is self-contained and already handles missing measurements gracefully; column-level checks already short-circuit to `NOT_EVALUATED` when their measurements are absent. No new fields, no per-check changes.

**Tech Stack:** Python 3.10+, pytest, postgres (test DB), soda-core.

**Spec:** `docs/superpowers/specs/2026-04-29-schema-check-survives-missing-column-design.md`

---

## File Map

| File | Change |
|---|---|
| `soda-core/src/soda_core/contracts/impl/contract_verification_impl.py` | Modify: wrap loop at lines 642-644 with try/except SodaCoreException |
| `soda-tests/tests/integration/test_schema_check_survives_missing_column.py` | Create: integration tests covering all five spec scenarios |

`SodaCoreException` is already imported at line 16 of `contract_verification_impl.py`. The module-level `logger` is already defined at line 60. No new imports required in the production file.

---

### Task 1: Failing integration test for the headline scenario

**Files:**
- Create: `soda-core/soda-tests/tests/integration/test_schema_check_survives_missing_column.py`

- [ ] **Step 1: Write the failing test**

Create `soda-core/soda-tests/tests/integration/test_schema_check_survives_missing_column.py` with this content:

```python
import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import CheckOutcome, ContractVerificationResult
from soda_core.contracts.impl.check_types.schema_check import SchemaCheckResult

# Table has only `id`. The contracts below reference `ghost_column` which does not exist —
# without the fix this triggers `column "ghost_column" does not exist` from the database
# and the entire scan aborts before the schema check can evaluate.
test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("schema_survives_missing")
    .column_varchar("id")
    .rows(rows=[("1",), ("2",)])
    .build()
)


def test_schema_check_runs_when_column_check_targets_missing_column(
    data_source_test_helper: DataSourceTestHelper,
):
    """Headline scenario from the bug report: schema + missing_count on a dropped column.

    With the fix, the schema check FAILs with `ghost_column` reported as missing and the
    `missing_count` check degrades to NOT_EVALUATED. Without the fix, the SQL exception
    propagates out of `verify()` and this test errors out before reaching the assertion.
    """
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: id
              - name: ghost_column
                checks:
                  - missing:
        """,
    )

    schema_results = [r for r in result.check_results if isinstance(r, SchemaCheckResult)]
    assert len(schema_results) == 1, "Schema check did not run — scan was aborted by aggregation failure"
    assert "ghost_column" in schema_results[0].expected_column_names_not_actual

    other_results = [r for r in result.check_results if not isinstance(r, SchemaCheckResult)]
    assert len(other_results) == 1
    assert other_results[0].outcome == CheckOutcome.NOT_EVALUATED
```

- [ ] **Step 2: Run the test against postgres and confirm it errors/fails**

Run:
```bash
cd /Users/mlukac/dev/soda/soda-python/soda-core
uv run pytest soda-tests/tests/integration/test_schema_check_survives_missing_column.py::test_schema_check_runs_when_column_check_targets_missing_column -v
```

Expected: test errors with `SodaCoreException: Could not execute aggregation query: column "ghost_column" does not exist` (raised from line 1813 of `contract_verification_impl.py` and re-raised at line 261). This confirms the bug reproduces.

---

### Task 2: Apply the fix

**Files:**
- Modify: `soda-core/soda-core/src/soda_core/contracts/impl/contract_verification_impl.py:642-644`

- [ ] **Step 1: Wrap the query execution loop**

Replace lines 642-644 of `soda-core/soda-core/src/soda_core/contracts/impl/contract_verification_impl.py`. The current code is:

```python
            # Executing the queries will set the value of the metrics linked to queries
            for query in self.queries:
                query_measurements: list[Measurement] = query.execute()
                measurements.extend(query_measurements)
```

Replace with:

```python
            # Executing the queries will set the value of the metrics linked to queries.
            # A SodaCoreException from one query (e.g. an aggregation referencing a column
            # that has been dropped) must not abort the scan — other queries, including the
            # schema query, still need to run so the user sees the real cause.
            for query in self.queries:
                try:
                    query_measurements: list[Measurement] = query.execute()
                    measurements.extend(query_measurements)
                except SodaCoreException as e:
                    logger.error(f"Query execution failed, continuing with remaining checks: {e}")
```

Notes:
- Catch is narrow: only `SodaCoreException`. Other exception types (programming bugs, infrastructure failures) still abort the scan.
- `logger` is the module-level logger already defined at line 60. ERROR-level entries land in `ContractVerificationResult.log_records` and surface through `get_errors()`.
- `SodaCoreException` is already imported at line 16; no new import needed.

- [ ] **Step 2: Re-run the test from Task 1 and confirm it passes**

Run:
```bash
cd /Users/mlukac/dev/soda/soda-python/soda-core
uv run pytest soda-tests/tests/integration/test_schema_check_survives_missing_column.py::test_schema_check_runs_when_column_check_targets_missing_column -v
```

Expected: PASS. The schema check ran, reported `ghost_column` as missing, and the `missing_count` check degraded to `NOT_EVALUATED`.

---

### Task 3: Add remaining integration tests

**Files:**
- Modify: `soda-core/soda-tests/tests/integration/test_schema_check_survives_missing_column.py`

- [ ] **Step 1: Add test for missing column without a schema check**

Append to the test file:

```python
def test_column_check_on_missing_column_without_schema_check(
    data_source_test_helper: DataSourceTestHelper,
):
    """Without a schema check defined, the column-level check still degrades cleanly
    rather than crashing the scan."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: ghost_column
                checks:
                  - missing:
        """,
    )

    assert len(result.check_results) == 1
    assert result.check_results[0].outcome == CheckOutcome.NOT_EVALUATED

    errors_str = "\n".join(str(r.msg) for r in (result.log_records or []) if r.levelno >= 40)
    assert "ghost_column" in errors_str, (
        f"Expected the SQL error referencing 'ghost_column' to appear in contract errors. "
        f"Got: {errors_str!r}"
    )
```

- [ ] **Step 2: Add test that one missing column does not poison checks on other columns**

Append:

```python
multi_column_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("schema_survives_partial_missing")
    .column_varchar("id")
    .column_integer("age")
    .rows(rows=[("1", 1), (None, 2), ("3", None)])
    .build()
)


def test_one_missing_column_does_not_poison_other_columns(
    data_source_test_helper: DataSourceTestHelper,
):
    """Checks on existing columns must continue to evaluate normally even when another
    column in the same contract has been dropped."""
    test_table = data_source_test_helper.ensure_test_table(multi_column_table_specification)

    result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: id
                checks:
                  - missing:
              - name: age
              - name: ghost_column
                checks:
                  - missing:
        """,
    )

    by_column = {}
    non_schema_results = [r for r in result.check_results if not isinstance(r, SchemaCheckResult)]
    for r in non_schema_results:
        by_column[r.check.column_name] = r.outcome

    # `id` has 1 missing value out of 3 -> missing check FAILS (default threshold must_be: 0)
    assert by_column.get("id") == CheckOutcome.FAILED, (
        f"Check on existing column should evaluate normally; got outcomes {by_column}"
    )
    # `ghost_column` doesn't exist -> its check degrades to NOT_EVALUATED
    assert by_column.get("ghost_column") == CheckOutcome.NOT_EVALUATED
```

Notes:
- The exact attribute path for "which column does this check belong to" may be `r.check.column_name` or similar — confirm during execution by inspecting `CheckResult` (defined in `contract_verification.py` near line 322). If the attribute name differs, adjust to whatever exposes the column name (e.g. `r.check.column` or matching by `r.check.path`).

- [ ] **Step 3: Add test that non-`SodaCoreException` still aborts**

Append:

```python
def test_non_sodacore_exception_still_aborts(
    data_source_test_helper: DataSourceTestHelper,
    monkeypatch: pytest.MonkeyPatch,
):
    """The wrap is intentionally narrow: only SodaCoreException is swallowed. Anything
    else (programming bugs, infrastructure failures) must still propagate so it gets
    noticed instead of silently masked."""
    from soda_core.contracts.impl.contract_verification_impl import AggregationQuery

    test_table = data_source_test_helper.ensure_test_table(multi_column_table_specification)

    def boom(self):
        raise ValueError("simulated programming bug")

    monkeypatch.setattr(AggregationQuery, "execute", boom)

    with pytest.raises(ValueError, match="simulated programming bug"):
        data_source_test_helper.assert_contract_fail(
            test_table=test_table,
            contract_yaml_str=f"""
                columns:
                  - name: id
                    checks:
                      - missing:
            """,
        )
```

- [ ] **Step 4: Run the full new test file and confirm all pass**

Run:
```bash
cd /Users/mlukac/dev/soda/soda-python/soda-core
uv run pytest soda-tests/tests/integration/test_schema_check_survives_missing_column.py -v
```

Expected: 4 tests pass.

If `test_one_missing_column_does_not_poison_other_columns` fails because of a wrong attribute name on `CheckResult`, open the helper or `contract_verification.py` to find the right accessor (likely `r.check.column_name` per existing tests; if not, look for the column reference on `r.check`) and adjust. Do NOT rewrite the test logic — only the attribute path.

---

### Task 4: Regression sweep

- [ ] **Step 1: Re-run the schema-check, missing-check, and invalid-check integration suites**

Run:
```bash
cd /Users/mlukac/dev/soda/soda-python/soda-core
uv run pytest soda-tests/tests/integration/test_schema_check.py soda-tests/tests/integration/test_missing_check.py soda-tests/tests/integration/test_invalid_check.py -v
```

Expected: all pre-existing tests still pass — the wrap is additive.

- [ ] **Step 2: Run pre-commit on the changed files**

Run:
```bash
cd /Users/mlukac/dev/soda/soda-python/soda-core
uv run pre-commit run --files \
  soda-core/src/soda_core/contracts/impl/contract_verification_impl.py \
  soda-tests/tests/integration/test_schema_check_survives_missing_column.py
```

Expected: PASS. Fix any formatting nits and re-run.

- [ ] **Step 3: Stage and commit**

Only run this step after the user explicitly authorizes the commit.

```bash
cd /Users/mlukac/dev/soda/soda-python/soda-core
git add \
  soda-core/src/soda_core/contracts/impl/contract_verification_impl.py \
  soda-tests/tests/integration/test_schema_check_survives_missing_column.py \
  docs/superpowers/specs/2026-04-29-schema-check-survives-missing-column-design.md \
  docs/superpowers/plans/2026-04-29-schema-check-survives-missing-column.md

git commit -m "$(cat <<'EOF'
fix(contracts): keep scan running so schema check reports dropped columns

A column-level check (missing/invalid/aggregate) on a column that has been
dropped from the table causes the aggregation SQL to fail. Until now that
SodaCoreException aborted the entire scan before the schema check could
evaluate, leaving the user staring at an opaque SQL error instead of the
clean "missing column" violation the schema check would have reported.

Wrap the query execution loop in ContractImpl.verify() with a narrow
try/except SodaCoreException that logs the underlying error to the contract
logger and continues to the next query. The schema check is self-contained
and reports the dropped column cleanly; dependent column-level checks
already short-circuit to NOT_EVALUATED on missing measurements. No new
fields, no per-check changes, no wire-format impact.

Reported by Tyler Adkins.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review Notes

- **Spec coverage:**
  - Wrap point at lines 642-644 → Task 2 Step 1 ✓
  - Schema check FAILED + dependent check NOT_EVALUATED → Task 1 ✓
  - Headline scenario (with schema check) → Task 1 ✓
  - Without schema check → Task 3 Step 1 ✓
  - One missing column does not poison others → Task 3 Step 2 ✓
  - Non-SodaCoreException still aborts → Task 3 Step 3 ✓
  - Existing tests still pass → Task 4 Step 1 ✓
  - Test #4 from spec (multiple checks on same missing column, error logs once) was dropped — too brittle to assert log-line cardinality across data sources, and the per-check NOT_EVALUATED behavior is already covered by Task 1 + Task 3 Step 2.
- **Placeholder scan:** none.
- **Type consistency:** `SchemaCheckResult.expected_column_names_not_actual: list[str]` is the documented field at `schema_check.py:336`. `CheckOutcome.NOT_EVALUATED` and `CheckOutcome.FAILED` exist (`contract_verification.py:219-224`). `SodaCoreException` and `logger` are imported at `contract_verification_impl.py:14-18` and `:60`.
