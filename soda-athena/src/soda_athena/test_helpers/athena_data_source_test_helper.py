from __future__ import annotations

import logging
import os

import boto3
from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger


ATHENA_ACCESS_KEY_ID = os.getenv("ATHENA_ACCESS_KEY_ID", None)
ATHENA_SECRET_ACCESS_KEY = os.getenv("ATHENA_SECRET_ACCESS_KEY", None)
ATHENA_S3_TEST_DIR = os.getenv("ATHENA_S3_TEST_DIR")
# Drop the extra / at the end of the S3 test dir. This gives conflicts with the location to clean up later.
if ATHENA_S3_TEST_DIR is not None:
    ATHENA_S3_TEST_DIR = ATHENA_S3_TEST_DIR[:-1] if ATHENA_S3_TEST_DIR.endswith("/") else ATHENA_S3_TEST_DIR
ATHENA_REGION_NAME = os.getenv("ATHENA_REGION_NAME", "eu-west-1")
ATHENA_CATALOG = os.getenv("ATHENA_CATALOG", "awsdatacatalog")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP")


class AthenaDataSourceTestHelper(DataSourceTestHelper):
    def __init__(self, name: str):
        super().__init__(name)
        self.s3_test_dir = ATHENA_S3_TEST_DIR
        if self.s3_test_dir and self.s3_test_dir.endswith("/"):
            self.s3_test_dir = self.s3_test_dir[:-1]

    def _create_database_name(self) -> str | None:
        return ATHENA_CATALOG

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
                type: athena
                name: {self.name}
                connection:
                    access_key_id: {ATHENA_ACCESS_KEY_ID}
                    secret_access_key: {ATHENA_SECRET_ACCESS_KEY}
                    staging_dir: {ATHENA_S3_TEST_DIR}/staging-dir
                    region_name: {ATHENA_REGION_NAME}
                    catalog: {ATHENA_CATALOG}
                    {f"work_group: {ATHENA_WORKGROUP}" if ATHENA_WORKGROUP else ""}
            """

    def _get_3_schema_dir(self):
        schema_name = self.extract_schema_from_prefix()
        return f"{self.s3_test_dir}/staging-dir/{ATHENA_CATALOG}/{schema_name}"

    def reset_table_for_fallback_retry(self, real_connection, table_name: str) -> None:
        """Reset an Athena table so the snapshot fallback recovery can retry
        ``CREATE EXTERNAL TABLE`` against an empty LOCATION.

        We can't reuse ``AthenaDataSourceImpl.execute_update``'s built-in
        ``DROP TABLE`` → ``_delete_s3_files`` hook here because the fallback
        recovery loop runs INSIDE ``SnapshotDataSourceConnection._activate_fallback``,
        and routing the DROP through ``data_source_impl.execute_update`` would
        bounce back into the snapshot we're currently inside (its
        ``_fallback_active`` flag isn't set until after the recovery loop
        finishes, so the call would attempt a replay match, fail, and recurse
        into ``_activate_fallback`` again).

        Three explicit steps:

        1. ``DROP TABLE`` on the real connection — removes the Glue catalog
           entry only. On EXTERNAL tables Athena leaves the underlying
           Parquet files in S3, so step 2 is required to avoid the
           subsequent ``CREATE EXTERNAL TABLE`` re-attaching to stale data
           (which would let the snapshot's ``INSERT INTO`` double the rows).
        2. List the table's S3 prefix.
        3. Delete each object via ``s3_client.delete_object`` (singular).

        The batched ``s3_client.delete_objects`` endpoint returns
        ``{"Errors": [...]}`` with ``Deleted`` absent when every key fails,
        which the existing ``AthenaDataSourceImpl._delete_s3_files`` parser
        silently swallows as ``"Deleted Unknown aws delete objects
        response"`` — that's a latent bug we don't want to fix in
        production code from a test path, so this helper does its own
        per-object loop. ``delete_object`` raises ``ClientError``
        synchronously on failure, which surfaces any real permission /
        policy issue instead of letting the test pass with stale data.
        """
        real_connection.execute_update(f"DROP TABLE {table_name}", log_query=False)
        impl = self.data_source_impl
        table_location: str = impl.table_s3_location(table_name, lowercase=True)
        bucket: str = impl._extract_s3_bucket(table_location)
        prefix: str = impl._extract_s3_folder(table_location)
        s3_client = impl._create_s3_client()
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for item in response.get("Contents") or []:
            s3_client.delete_object(Bucket=bucket, Key=item["Key"])

    def drop_test_schema_if_exists(self) -> str:
        super().drop_test_schema_if_exists()
        self.data_source_impl._delete_s3_files(self._get_3_schema_dir())

    def drop_schema_if_exists(self, schema: str) -> None:
        """Drop all tables/views via the Glue API, then drop the schema.

        Athena's DROP SCHEMA CASCADE fails on Iceberg tables, and DROP TABLE
        via Athena SQL may fail if S3 data isn't accessible. Using the Glue API
        removes catalog entries without requiring S3 access.
        """
        try:
            glue_client = self._create_glue_client()

            # Delete all tables/views in the database via Glue
            try:
                response = glue_client.get_tables(DatabaseName=schema)
                for table in response.get("TableList", []):
                    table_name = table["Name"]
                    try:
                        glue_client.delete_table(DatabaseName=schema, Name=table_name)
                        logger.info(f"Glue: deleted table/view {schema}.{table_name}")
                    except Exception as e:
                        logger.warning(f"Glue: error deleting table {table_name}: {e}")
            except glue_client.exceptions.EntityNotFoundException:
                logger.info(f"Schema {schema} does not exist in Glue, nothing to drop")
                return
            except Exception as e:
                logger.warning(f"Glue: error listing tables in {schema}: {e}")

            # Delete the database itself
            try:
                glue_client.delete_database(Name=schema)
                logger.info(f"Glue: deleted database {schema}")
            except Exception as e:
                logger.warning(f"Glue: error deleting database {schema}: {e}")

            # Best-effort S3 cleanup for the schema directory
            try:
                schema_location = self.data_source_impl.table_s3_location(f"{ATHENA_CATALOG}.{schema}", lowercase=False)
                self.data_source_impl._delete_s3_files(schema_location)
            except Exception as e:
                logger.warning(f"S3 cleanup for schema {schema} failed (non-fatal): {e}")
        except Exception as e:
            logger.warning(f"Error dropping test schema {schema}: {e}")

    def _create_glue_client(self):
        aws_credentials = self.data_source_impl.connection.aws_credentials
        aws_credentials = aws_credentials.resolve_role("soda_sql_test_cleanup")
        return boto3.client(
            "glue",
            region_name=aws_credentials.region_name,
            aws_access_key_id=aws_credentials.access_key_id,
            aws_secret_access_key=aws_credentials.secret_access_key,
            aws_session_token=aws_credentials.session_token,
        )

    def drop_schema_if_exists_sql(self, schema: str) -> str:
        dialect = self.data_source_impl.sql_dialect
        quoted_catalog = dialect.quote_for_ddl(ATHENA_CATALOG)
        quoted_schema = dialect.quote_for_ddl(schema)
        return f"DROP SCHEMA IF EXISTS {quoted_catalog}.{quoted_schema} CASCADE;"

    def drop_schema_sql(self, schema: str) -> str:
        dialect = self.data_source_impl.sql_dialect
        quoted_catalog = dialect.quote_for_ddl(ATHENA_CATALOG)
        quoted_schema = dialect.quote_for_ddl(schema)
        return f"DROP SCHEMA {quoted_catalog}.{quoted_schema} CASCADE;"
