"""Integration test for Athena S3 Tables catalogs with '/' in the catalog name.

Requires:
    - ATHENA_S3_TABLES_CATALOG env var set to a catalog like 's3tablescatalog/my-bucket'
    - AWS credentials with access to the S3 Tables catalog
    - The standard ATHENA_* env vars for the data source connection

S3 Tables catalogs only support Iceberg tables, so this test bypasses the normal
test infrastructure (which creates external Parquet tables) and uses raw SQL.
"""

from __future__ import annotations

import logging
import os
import random
import string

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger

ATHENA_S3_TABLES_CATALOG = os.getenv("ATHENA_S3_TABLES_CATALOG")

pytestmark = [
    pytest.mark.skipif(
        ATHENA_S3_TABLES_CATALOG is None,
        reason="ATHENA_S3_TABLES_CATALOG env var not set — skipping S3 Tables integration tests",
    ),
    pytest.mark.no_snapshot,
]

SCHEMA_NAME = "soda_s3tables_test"
TABLE_SUFFIX = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
TABLE_NAME = f"sodatest_slash_catalog_{TABLE_SUFFIX}"


@pytest.fixture(scope="module")
def s3_tables_setup(data_source_test_helper_session: DataSourceTestHelper):
    """Create a schema and Iceberg table in the S3 Tables catalog, yield, then clean up."""
    dialect = data_source_test_helper_session.data_source_impl.sql_dialect
    ds = data_source_test_helper_session.data_source_impl

    catalog_quoted = dialect.quote_for_ddl(ATHENA_S3_TABLES_CATALOG)
    schema_quoted = dialect.quote_for_ddl(SCHEMA_NAME)
    table_quoted = dialect.quote_for_ddl(TABLE_NAME)
    fqn = f"{catalog_quoted}.{schema_quoted}.{table_quoted}"

    # Create schema (namespace) in the S3 Tables catalog
    ds.execute_update(f"CREATE DATABASE IF NOT EXISTS {catalog_quoted}.{schema_quoted};")
    logger.info(f"Created schema {catalog_quoted}.{schema_quoted}")

    # Create an Iceberg table (S3 Tables only supports Iceberg)
    ds.execute_update(
        f"CREATE TABLE IF NOT EXISTS {fqn} ("
        f"  id string,"
        f"  country string,"
        f"  value integer"
        f") TBLPROPERTIES ('table_type'='ICEBERG');"
    )
    logger.info(f"Created Iceberg table {fqn}")

    # Insert test data
    ds.execute_update(f"INSERT INTO {fqn} VALUES" f" ('1', 'be', 10)," f" ('2', 'us', 20)," f" ('3', 'fr', 30);")
    logger.info(f"Inserted test rows into {fqn}")

    yield {
        "catalog": ATHENA_S3_TABLES_CATALOG,
        "schema": SCHEMA_NAME,
        "table": TABLE_NAME,
    }

    # Cleanup
    try:
        ds.execute_update(f"DROP TABLE IF EXISTS {fqn};")
        logger.info(f"Dropped table {fqn}")
    except Exception as e:
        logger.warning(f"Error dropping table {fqn}: {e}")


def test_row_count_with_slash_catalog(
    data_source_test_helper: DataSourceTestHelper,
    s3_tables_setup: dict,
):
    """Contract verify with a DQN containing '/' in the catalog — real Athena query."""
    catalog = s3_tables_setup["catalog"]
    schema = s3_tables_setup["schema"]
    table = s3_tables_setup["table"]
    ds_name = data_source_test_helper.data_source_impl.name

    # DQN with '/' in the catalog: "ds_name/s3tablescatalog/bucket/schema/table"
    dqn = f"{ds_name}/{catalog}/{schema}/{table}"

    result = data_source_test_helper.verify_contract(
        contract_yaml_str=f"""
            dataset: {dqn}
            columns: []
            checks:
              - row_count:
                  threshold:
                    must_be: 3
        """,
        test_table=None,
    )

    assert result.is_passed, (
        f"Contract verification should pass for DQN '{dqn}'. "
        f"Errors: {result.logs.get_errors_str() if hasattr(result, 'logs') else 'N/A'}"
    )


def test_column_metadata_with_slash_catalog(
    data_source_test_helper: DataSourceTestHelper,
    s3_tables_setup: dict,
):
    """Column metadata query works for a table in an S3 Tables catalog with '/' in the name."""
    catalog = s3_tables_setup["catalog"]
    schema = s3_tables_setup["schema"]
    table = s3_tables_setup["table"]
    ds = data_source_test_helper.data_source_impl

    # Simulate what skeleton generation / copilot does: parse the DQN, extract prefixes
    from soda_core.common.dataset_identifier import DatasetIdentifier

    ds_name = ds.name
    dqn = f"{ds_name}/{catalog}/{schema}/{table}"
    identifier = DatasetIdentifier.parse(dqn)

    # This calls extract_database_from_prefix / extract_schema_from_prefix internally
    columns = ds.get_columns_metadata(
        dataset_prefixes=identifier.prefixes,
        dataset_name=identifier.dataset_name,
    )

    column_names = [col.name for col in columns]
    assert "id" in column_names, f"Expected 'id' column, got: {column_names}"
    assert "country" in column_names, f"Expected 'country' column, got: {column_names}"
    assert "value" in column_names, f"Expected 'value' column, got: {column_names}"
