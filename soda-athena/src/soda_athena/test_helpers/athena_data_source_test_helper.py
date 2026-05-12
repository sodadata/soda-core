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
        """Athena needs an extra S3 cleanup step the base implementation can't
        deliver during snapshot fallback recovery.

        AthenaDataSourceImpl.execute_update intercepts ``DROP TABLE`` to also
        ``_delete_s3_files`` the underlying location — that's the path used by
        regular test cleanup. We can't reuse it here because the fallback
        recovery loop runs INSIDE ``SnapshotDataSourceConnection._activate_fallback``,
        and routing the DROP through ``data_source_impl.execute_update`` would
        bounce back into the snapshot we're currently inside (the snapshot's
        ``_fallback_active`` flag isn't set until after the recovery loop
        finishes, so the call would attempt a replay match, fail, and recurse
        into ``_activate_fallback`` again).

        So we replicate the two-step cleanup explicitly:

        1. ``DROP TABLE`` on the real connection → removes the Glue catalog
           entry. On Athena this does NOT remove the table's S3 files because
           we're talking to the connection directly, bypassing the Impl hook.
        2. Manual ``_delete_s3_files`` against the same path that
           ``AthenaDataSourceImpl.execute_update`` would have used. Otherwise
           the subsequent ``CREATE EXTERNAL TABLE`` re-attaches to the
           previous LOCATION's stale Parquet files and the snapshot's INSERT
           doubles the data.
        """
        real_connection.execute_update(f"DROP TABLE {table_name}", log_query=False)
        # table_s3_location is public; _delete_s3_files is "private" by naming
        # convention but already used externally by drop_test_schema_if_exists
        # for the same reason — there isn't a public equivalent.
        table_location: str = self.data_source_impl.table_s3_location(table_name, lowercase=True)
        self.data_source_impl._delete_s3_files(table_location)

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
