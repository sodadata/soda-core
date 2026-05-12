from __future__ import annotations

import logging
import os
import re

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
        """Athena-specific fallback-recovery strategy: drop the Glue catalog
        entry and **skip** the snapshot's ``INSERT INTO`` for this table.

        Why this differs from the generic DROP+re-CREATE flow other DBs use:

        * Repeated CI runs showed that clearing the table's S3 LOCATION
          between the failed ``CREATE EXTERNAL TABLE`` and the snapshot's
          ``INSERT INTO`` is unreliable on Athena — both the batched
          ``_delete_s3_files`` path (silently swallows AWS ``Errors``-only
          responses as "Unknown") and per-object ``delete_object`` left
          stale Parquet files behind. End result was consistently 2× the
          expected row count.
        * ``_LazyRealConnectionFactory._materialize_pending_tables`` has
          already CREATE+INSERTed the test table on the real DB using the
          **exact** same ``test_table_specification`` that produced the
          snapshot's recorded ops. By the time the recovery loop hits the
          recorded CREATE, the table is *already in the correct test state*
          — the snapshot's recorded INSERT just duplicates the rows the
          factory already wrote.

        Approach:

        1. Glue ``delete_table`` removes the catalog entry cleanly via a
           non-pyathena boto3 client (so the snapshot recovery loop's retry
           ``CREATE EXTERNAL TABLE`` succeeds rather than raising
           ``AlreadyExistsException``). Best-effort: if Glue rejects the
           call we log and continue — the INSERT-skip step still prevents
           data doubling.
        2. We deliberately **do not touch S3**. The factory's Parquet files
           at the LOCATION are the correct test state; we want the
           re-attached EXTERNAL TABLE to read them.
        3. Monkey-patch ``real_connection.execute_update`` so the snapshot
           recovery loop's next ``INSERT INTO`` targeting this specific
           table is a no-op. The patch self-restores after the intercepted
           INSERT so subsequent operations (the same loop's other ops, or
           later real-DB calls) run normally.

        Other data sources keep their existing DROP+re-CREATE+INSERT
        recovery — they don't have the S3 unreliability issue, and atomic
        DROP+CREATE works fine for them.
        """
        cleaned = table_name.replace("`", "").replace('"', "")
        parts = cleaned.split(".")
        if len(parts) < 2:
            logger.info(f"ATHENA-RESET: skip — FQN {table_name!r} has fewer than 2 parts; no-op")
            return
        glue_schema = parts[-2].lower()
        glue_table = parts[-1].lower()
        cleaned_target = cleaned.lower()
        logger.info(
            f"ATHENA-RESET: handling fallback CREATE failure for {cleaned_target} via "
            "Glue catalog drop + INSERT-skip (Athena-specific to avoid S3 doubling)"
        )

        # Step 1: drop the Glue catalog entry so the recovery loop's retry
        # CREATE EXTERNAL TABLE succeeds. Glue API bypasses pyathena entirely.
        glue_client = self._create_glue_client()
        try:
            glue_client.delete_table(DatabaseName=glue_schema, Name=glue_table)
            logger.info(f"ATHENA-RESET: glue.delete_table OK — DatabaseName={glue_schema} Name={glue_table}")
        except glue_client.exceptions.EntityNotFoundException:
            logger.info(
                f"ATHENA-RESET: glue.delete_table EntityNotFoundException (already gone) — "
                f"DatabaseName={glue_schema} Name={glue_table}"
            )
        except Exception as glue_exc:
            # Best-effort — if Glue delete fails, the snapshot's retry CREATE
            # will raise AlreadyExists (logged as a warning), then our
            # INSERT-skip below still prevents data doubling.
            logger.warning(
                f"ATHENA-RESET: glue.delete_table FAILED (continuing with INSERT-skip) — "
                f"DatabaseName={glue_schema} Name={glue_table}: {type(glue_exc).__name__}: {glue_exc}"
            )

        # Step 2: patch real_connection.execute_update so the snapshot's
        # next INSERT into this specific table is a no-op. The patch
        # self-restores after the intercepted call.
        original_execute_update = real_connection.execute_update

        def _patched_execute_update(sql: str, log_query: bool = True) -> int:
            sql_upper = sql.lstrip().upper()
            if sql_upper.startswith("INSERT INTO "):
                # CREATE uses backticks (DDL), INSERT uses double quotes
                # (DML); both normalise to the same cleaned string.
                cleaned_sql = sql.replace("`", "").replace('"', "").lower()
                match = re.match(r"\s*insert\s+into\s+([^\s(]+)", cleaned_sql)
                if match is not None and match.group(1) == cleaned_target:
                    logger.info(
                        f"ATHENA-RESET: intercepted snapshot INSERT INTO {cleaned_target} — "
                        "skipping (lazy factory already populated this table)"
                    )
                    real_connection.execute_update = original_execute_update
                    return 0
            return original_execute_update(sql, log_query=log_query)

        real_connection.execute_update = _patched_execute_update

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
