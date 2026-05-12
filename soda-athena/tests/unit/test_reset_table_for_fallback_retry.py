"""Unit tests for AthenaDataSourceTestHelper.reset_table_for_fallback_retry.

This helper override is invoked from SnapshotDataSourceConnection._activate_fallback
when fallback recovery's CREATE TABLE fails because the lazy factory has already
created the table. It needs to:

* drop the Glue catalog entry via the Glue API (NOT SQL DROP TABLE — Athena's
  SQL DROP TABLE path leaves the door open for the snapshot's recovery loop
  to re-enter the snapshot connection, and the existing batched-delete S3
  cleanup silently swallows AWS Errors-only responses as "Unknown" which was
  causing data doubling),
* clear the table's S3 LOCATION via per-object delete_object so any real
  permission/policy failure raises ClientError instead of being silently
  ignored.

These tests don't talk to real AWS — they wire mock boto3 clients into the
helper and assert the exact API calls + arguments. That's the only way to
verify the flow without an AWS account, and it pins the boundary contract
so a future refactor of the helper won't accidentally regress to the
silent-failure behaviour.
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
def mock_helper() -> Any:
    """Build an AthenaDataSourceTestHelper instance with mocked boto3 clients
    and a mocked data_source_impl. The real __init__ pulls connection
    properties from the environment, so we construct a bare object via
    ``__new__`` and inject the attributes the method-under-test reads."""
    helper = AthenaDataSourceTestHelper.__new__(AthenaDataSourceTestHelper)

    glue_client = MagicMock()
    # Provide an EntityNotFoundException class so the helper's `except` block
    # can match against it; default boto3 client gives this attribute on .exceptions.
    glue_client.exceptions.EntityNotFoundException = type("EntityNotFoundException", (Exception,), {})

    s3_client = MagicMock()
    s3_client.get_paginator.return_value.paginate.return_value = iter(
        [{"Contents": [{"Key": "staging-dir/awsdatacatalog/myschema/sodatest_xxx/data.parquet"}]}]
    )

    # AthenaDataSourceImpl methods the helper relies on.
    impl = MagicMock()
    impl.table_s3_location.return_value = "s3://my-bucket/staging-dir/awsdatacatalog/myschema/sodatest_xxx/"
    impl._extract_s3_bucket.return_value = "my-bucket"
    impl._extract_s3_folder.return_value = "staging-dir/awsdatacatalog/myschema/sodatest_xxx/"
    impl._create_s3_client.return_value = s3_client

    helper.data_source_impl = impl
    helper._glue_client_for_test = glue_client  # captured for assertions
    # Replace _create_glue_client to return our mock.
    helper._create_glue_client = lambda: glue_client
    return helper


def test_calls_glue_delete_table_with_lowercase_schema_and_name(mock_helper):
    """Glue identifiers are stored lowercase; the snapshot's CREATE EXTERNAL
    TABLE contains a mixed-case FQN (``SODATEST_xxx``). The helper must
    normalise both schema and table parts before talking to Glue."""
    table_name = "`awsdatacatalog`.`myschema`.`SODATEST_xxx`"

    mock_helper.reset_table_for_fallback_retry(real_connection=MagicMock(), table_name=table_name)

    mock_helper._glue_client_for_test.delete_table.assert_called_once_with(
        DatabaseName="myschema",
        Name="sodatest_xxx",
    )


def test_swallows_entity_not_found_so_s3_cleanup_still_runs(mock_helper):
    """If the Glue table entry doesn't exist (e.g., a previous attempt
    already removed it), we should still proceed with S3 cleanup rather
    than aborting on EntityNotFoundException."""
    not_found = mock_helper._glue_client_for_test.exceptions.EntityNotFoundException
    mock_helper._glue_client_for_test.delete_table.side_effect = not_found("not found")

    mock_helper.reset_table_for_fallback_retry(real_connection=MagicMock(), table_name="`cat`.`schema`.`tbl`")

    # S3 cleanup must still run — list and per-key delete.
    s3_client = mock_helper.data_source_impl._create_s3_client.return_value
    s3_client.get_paginator.assert_called_once_with("list_objects_v2")
    s3_client.delete_object.assert_called_once_with(
        Bucket="my-bucket",
        Key="staging-dir/awsdatacatalog/myschema/sodatest_xxx/data.parquet",
    )


def test_propagates_unexpected_glue_error(mock_helper):
    """Glue errors other than EntityNotFoundException — e.g., transient API
    failures or permission issues — must propagate. Silent recovery here
    would let us re-attach to a stale catalog entry and double the data
    again, the exact scenario this helper exists to prevent."""
    mock_helper._glue_client_for_test.delete_table.side_effect = ClientError(
        error_response={"Error": {"Code": "AccessDenied", "Message": "denied"}},
        operation_name="DeleteTable",
    )

    with pytest.raises(ClientError):
        mock_helper.reset_table_for_fallback_retry(real_connection=MagicMock(), table_name="`cat`.`schema`.`tbl`")


def test_calls_delete_object_per_key_under_table_prefix(mock_helper):
    """Each S3 key returned by list_objects_v2 must trigger an individual
    delete_object call — NOT the batched delete_objects endpoint. The
    batched endpoint silently swallows AWS's Errors-only response as
    "Unknown" in the existing _delete_s3_files; per-key delete raises
    ClientError on failure so a real problem actually surfaces."""
    # Two pages, three keys total.
    s3_client = mock_helper.data_source_impl._create_s3_client.return_value
    s3_client.get_paginator.return_value.paginate.return_value = iter(
        [
            {"Contents": [{"Key": "a.parquet"}, {"Key": "b.parquet"}]},
            {"Contents": [{"Key": "c.parquet"}]},
        ]
    )

    mock_helper.reset_table_for_fallback_retry(real_connection=MagicMock(), table_name="`cat`.`schema`.`tbl`")

    # delete_object is called per key — NOT delete_objects (which is what
    # the existing _delete_s3_files uses).
    assert s3_client.delete_object.call_count == 3
    assert s3_client.delete_objects.call_count == 0
    deleted_keys = {c.kwargs["Key"] for c in s3_client.delete_object.call_args_list}
    assert deleted_keys == {"a.parquet", "b.parquet", "c.parquet"}


def test_skips_when_listing_returns_no_contents(mock_helper):
    """An empty S3 prefix (nothing to clean) is not an error — the helper
    must complete cleanly so subsequent recovery steps proceed."""
    s3_client = mock_helper.data_source_impl._create_s3_client.return_value
    s3_client.get_paginator.return_value.paginate.return_value = iter([{}])  # no Contents

    mock_helper.reset_table_for_fallback_retry(real_connection=MagicMock(), table_name="`cat`.`schema`.`tbl`")

    assert s3_client.delete_object.call_count == 0


def test_s3_delete_failure_propagates(mock_helper):
    """If the per-object delete fails (e.g., real permission denial), the
    error must propagate. The whole point of switching off the batched
    delete_objects + silent-failure parser is to surface this kind of
    issue so it shows up as a test failure with a clear cause, rather
    than as confusing data-doubling downstream."""
    s3_client = mock_helper.data_source_impl._create_s3_client.return_value
    s3_client.delete_object.side_effect = ClientError(
        error_response={"Error": {"Code": "AccessDenied", "Message": "denied"}},
        operation_name="DeleteObject",
    )

    with pytest.raises(ClientError):
        mock_helper.reset_table_for_fallback_retry(real_connection=MagicMock(), table_name="`cat`.`schema`.`tbl`")


def test_short_fqn_with_only_two_parts(mock_helper):
    """The FQN can legitimately come in as ``schema.table`` (no catalog) on
    some code paths. The helper should still extract schema + table and
    not blow up when there are fewer than three dot-separated parts."""
    mock_helper.reset_table_for_fallback_retry(real_connection=MagicMock(), table_name="`myschema`.`SODATEST_xxx`")

    mock_helper._glue_client_for_test.delete_table.assert_called_once_with(
        DatabaseName="myschema",
        Name="sodatest_xxx",
    )


def test_single_part_fqn_is_a_noop(mock_helper):
    """A single-part FQN (``table`` with no schema) can't be addressed by
    Glue's DatabaseName/Name API — the helper short-circuits and skips
    both the catalog drop and the S3 cleanup."""
    mock_helper.reset_table_for_fallback_retry(real_connection=MagicMock(), table_name="`SODATEST_xxx`")

    mock_helper._glue_client_for_test.delete_table.assert_not_called()
    mock_helper.data_source_impl._create_s3_client.assert_not_called()
