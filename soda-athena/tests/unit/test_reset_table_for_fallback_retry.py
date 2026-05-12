"""Unit tests for AthenaDataSourceTestHelper.reset_table_for_fallback_retry.

The Athena override takes a different recovery strategy from every other
data source: instead of DROP+re-CREATE the table (which on Athena ran into
unreliable S3 cleanup → 2× row doubling), it drops the Glue catalog entry
and skips the snapshot's recorded INSERT INTO this table. The lazy real-
connection factory has already populated the table with the correct
``test_table_specification`` data, so the snapshot's INSERT is redundant —
skipping it is what prevents the doubling.

These tests pin that contract end-to-end against mocked boto3 clients and
a mocked real_connection (no real AWS, no real DBAPI).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError
from soda_athena.test_helpers.athena_data_source_test_helper import (
    AthenaDataSourceTestHelper,
)


@pytest.fixture
def helper() -> Any:
    """Build an AthenaDataSourceTestHelper instance with mocked AWS clients
    and a mocked data_source_impl. The real __init__ pulls connection
    properties from the environment, so we bypass it with ``__new__`` and
    inject only the attributes ``reset_table_for_fallback_retry`` touches."""
    h = AthenaDataSourceTestHelper.__new__(AthenaDataSourceTestHelper)

    glue_client = MagicMock()
    glue_client.exceptions.EntityNotFoundException = type("EntityNotFoundException", (Exception,), {})

    h.data_source_impl = MagicMock()
    h._glue_client_for_test = glue_client
    h._create_glue_client = lambda: glue_client
    return h


def test_glue_delete_table_called_with_lowercase_schema_and_name(helper):
    """Glue identifiers are stored lowercase. The snapshot's CREATE EXTERNAL
    TABLE has a mixed-case FQN (``SODATEST_xxx``) — we must normalise both
    schema and table parts before calling Glue."""
    real_conn = MagicMock()
    helper.reset_table_for_fallback_retry(
        real_connection=real_conn,
        table_name="`awsdatacatalog`.`MySchema`.`SODATEST_xxx`",
    )

    helper._glue_client_for_test.delete_table.assert_called_once_with(
        DatabaseName="myschema",
        Name="sodatest_xxx",
    )


def test_glue_entity_not_found_is_swallowed_and_patch_still_installs(helper):
    """If the catalog entry is already gone, the recovery is still meaningful:
    the patch must install so that any later INSERT to this table from the
    snapshot's recovery loop is skipped."""
    not_found = helper._glue_client_for_test.exceptions.EntityNotFoundException
    helper._glue_client_for_test.delete_table.side_effect = not_found("already gone")

    real_conn = MagicMock()
    original_execute_update = real_conn.execute_update
    helper.reset_table_for_fallback_retry(
        real_connection=real_conn,
        table_name="`cat`.`schema`.`tbl`",
    )

    # Patch is installed (execute_update was replaced).
    assert real_conn.execute_update is not original_execute_update


def test_glue_other_errors_are_logged_but_not_raised(helper):
    """A real Glue failure (e.g. AccessDenied) must NOT raise — the
    INSERT-skip step is the actual safety net against doubling, and it
    must still get installed even if Glue rejected the catalog drop."""
    helper._glue_client_for_test.delete_table.side_effect = ClientError(
        error_response={"Error": {"Code": "AccessDenied", "Message": "denied"}},
        operation_name="DeleteTable",
    )

    real_conn = MagicMock()
    original_execute_update = real_conn.execute_update

    # Must not raise.
    helper.reset_table_for_fallback_retry(
        real_connection=real_conn,
        table_name="`cat`.`schema`.`tbl`",
    )

    # Patch is installed despite the Glue error.
    assert real_conn.execute_update is not original_execute_update


def test_patched_execute_update_skips_insert_into_target_table(helper):
    """The whole point: an ``INSERT INTO <target>`` that comes through the
    real connection after the helper runs must be a no-op. The lazy
    factory already wrote the correct rows; running the snapshot's INSERT
    would double them."""
    real_conn = MagicMock()
    original = real_conn.execute_update

    helper.reset_table_for_fallback_retry(
        real_connection=real_conn,
        table_name="`cat`.`schema`.`tbl`",
    )

    # INSERT INTO using DML quoting (what the snapshot's INSERT looks like).
    rc = real_conn.execute_update('INSERT INTO "cat"."schema"."tbl" ("col") VALUES (1)')

    # The patched version returns 0 and does NOT delegate to the original.
    assert rc == 0
    original.assert_not_called()


def test_patched_execute_update_passes_through_other_inserts(helper):
    """An INSERT targeting a *different* table must run normally — we only
    want to skip the redundant INSERT for the table whose CREATE just
    failed (the one the lazy factory pre-populated)."""
    real_conn = MagicMock()
    original = real_conn.execute_update

    helper.reset_table_for_fallback_retry(
        real_connection=real_conn,
        table_name="`cat`.`schema`.`SODATEST_target`",
    )

    real_conn.execute_update('INSERT INTO "cat"."schema"."SODATEST_other" VALUES (1)')

    original.assert_called_once_with(
        'INSERT INTO "cat"."schema"."SODATEST_other" VALUES (1)',
        log_query=True,
    )


def test_patched_execute_update_passes_through_non_insert_sql(helper):
    """The patch must not intercept anything other than INSERT INTO — DROP,
    CREATE, SELECT, etc. all go through unchanged. The recovery loop's
    retry ``CREATE EXTERNAL TABLE`` runs through here too; it must
    succeed so the snapshot mismatch test can continue against a real
    table catalog entry."""
    real_conn = MagicMock()
    original = real_conn.execute_update

    helper.reset_table_for_fallback_retry(
        real_connection=real_conn,
        table_name="`cat`.`schema`.`tbl`",
    )

    real_conn.execute_update("CREATE EXTERNAL TABLE `cat`.`schema`.`tbl` (col int)")

    original.assert_called_once()


def test_patch_self_restores_after_intercepted_insert(helper):
    """After the patched execute_update intercepts and skips one INSERT to
    the target table, it must remove itself so subsequent operations
    (e.g. an unrelated INSERT to the same table later in the test
    session) run normally."""
    real_conn = MagicMock()
    original = real_conn.execute_update

    helper.reset_table_for_fallback_retry(
        real_connection=real_conn,
        table_name="`cat`.`schema`.`tbl`",
    )

    # First INSERT — skipped.
    rc1 = real_conn.execute_update('INSERT INTO "cat"."schema"."tbl" VALUES (1)')
    assert rc1 == 0
    original.assert_not_called()

    # Patch should have self-restored.
    assert real_conn.execute_update is original

    # Second INSERT — passes through.
    real_conn.execute_update('INSERT INTO "cat"."schema"."tbl" VALUES (2)')
    original.assert_called_once_with(
        'INSERT INTO "cat"."schema"."tbl" VALUES (2)',
    )


def test_short_fqn_is_a_noop(helper):
    """A single-part FQN can't be routed to Glue's DatabaseName/Name API.
    The helper must return early without dropping the catalog entry or
    patching execute_update — there's nothing useful it can do."""
    real_conn = MagicMock()
    original = real_conn.execute_update

    helper.reset_table_for_fallback_retry(
        real_connection=real_conn,
        table_name="`tbl`",
    )

    helper._glue_client_for_test.delete_table.assert_not_called()
    # execute_update was not replaced.
    assert real_conn.execute_update is original


def test_insert_match_is_case_insensitive_and_quote_style_insensitive(helper):
    """The snapshot's CREATE statement uses backticks (DDL), the snapshot's
    INSERT uses double quotes (DML), and the table name has mixed case.
    The match must normalise all of these so the right INSERT gets
    intercepted."""
    real_conn = MagicMock()
    original = real_conn.execute_update

    # Target supplied with backticks + mixed case (the CREATE statement form).
    helper.reset_table_for_fallback_retry(
        real_connection=real_conn,
        table_name="`AwsDataCatalog`.`MySchema`.`SODATEST_target`",
    )

    # Snapshot's INSERT comes through with double quotes + the same letters
    # in their original casing.
    rc = real_conn.execute_update('INSERT INTO "AwsDataCatalog"."MySchema"."SODATEST_target" VALUES (1)')

    assert rc == 0
    original.assert_not_called()
