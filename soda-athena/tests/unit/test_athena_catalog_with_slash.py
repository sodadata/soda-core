"""E2E flow tests for Athena catalogs containing '/' in the name.

AWS S3 Tables catalogs use names like 's3tablescatalog/bucket_name'. The DQN
(Data Quality Name) format uses '/' as separator, so DatasetIdentifier.parse()
over-splits the catalog into multiple prefix elements.

These tests verify the complete flow from DQN string → DatasetIdentifier.parse()
→ Athena prefix extraction → SQL generation produces correct results for all
affected paths: contract verify queries, column metadata queries, and DDL.
"""

from __future__ import annotations

from soda_athena.common.data_sources.athena_data_source import (
    AthenaDataSourceImpl,
    AthenaSqlDialect,
)
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.metadata_types import DbSchemaDataSourceNamespace
from soda_core.common.sql_ast import CTE, SELECT, STAR, WITH
from soda_core.common.sql_dialect import FROM

S3_TABLES_CATALOG = "s3tablescatalog/my_bucket"
SCHEMA = "my_schema"
TABLE = "my_table"
DQN = f"athena_ds/{S3_TABLES_CATALOG}/{SCHEMA}/{TABLE}"


class TestE2EContractVerifyFlowWithSlashCatalog:
    """Simulate the contract verify flow: DQN parsing → CTE/FROM → SQL generation."""

    def test_dqn_parsing_produces_oversplit_prefixes(self):
        """DatasetIdentifier.parse() splits the catalog on '/', producing 3 prefixes."""
        identifier = DatasetIdentifier.parse(DQN)
        assert identifier.data_source_name == "athena_ds"
        assert identifier.dataset_name == TABLE
        # The catalog 's3tablescatalog/my_bucket' is split into 2 prefix elements
        assert identifier.prefixes == ["s3tablescatalog", "my_bucket", SCHEMA]

    def test_contract_verify_from_clause(self):
        """The FROM clause in contract verify CTE uses the parsed prefixes.

        ContractVerificationImpl builds:
            CTE("_soda_filtered_dataset").AS([
                SELECT(STAR()),
                FROM(dataset_identifier.dataset_name, dataset_identifier.prefixes),
            ])
        """
        identifier = DatasetIdentifier.parse(DQN)
        dialect = AthenaSqlDialect()

        sql = dialect.build_select_sql(
            [
                SELECT(STAR()),
                FROM(identifier.dataset_name, identifier.prefixes),
            ]
        )

        # The Athena dialect should collapse the over-split prefixes back
        assert f'"s3tablescatalog/my_bucket"."{SCHEMA}"."{TABLE}"' in sql
        # Must NOT produce a 4-part name
        assert sql.count('"."') == 2  # exactly 2 dot-separators between 3 quoted parts

    def test_contract_verify_cte_generation(self):
        """Full CTE generation as done by ContractVerificationImpl (WITH ... AS ...)."""
        identifier = DatasetIdentifier.parse(DQN)
        dialect = AthenaSqlDialect()

        cte = CTE("_soda_filtered_dataset").AS(
            [
                SELECT(STAR()),
                FROM(identifier.dataset_name, identifier.prefixes),
            ]
        )

        sql = dialect.build_select_sql(
            [
                WITH([cte]),
                SELECT(STAR()),
                FROM("_soda_filtered_dataset"),
            ]
        )

        # The CTE definition should contain the correctly collapsed catalog
        assert f'"s3tablescatalog/my_bucket"."{SCHEMA}"."{TABLE}"' in sql
        # The outer query references the CTE alias
        assert '"_soda_filtered_dataset"' in sql


class TestE2EColumnMetadataFlowWithSlashCatalog:
    """Simulate the column metadata flow: DQN parsing → namespace → information_schema query."""

    def test_prefix_extraction_gives_correct_catalog_and_schema(self):
        """AthenaDataSourceImpl extracts the correct catalog and schema from over-split prefixes."""
        identifier = DatasetIdentifier.parse(DQN)
        # Simulate what DataSourceImpl._build_columns_metadata_namespace does,
        # but using Athena's overridden extract methods
        catalog, schema = AthenaDataSourceImpl._normalize_athena_prefixes(None, identifier.prefixes)
        assert catalog == S3_TABLES_CATALOG
        assert schema == SCHEMA

    def test_metadata_query_from_clause(self):
        """The metadata query FROM clause references the correct catalog's information_schema."""
        identifier = DatasetIdentifier.parse(DQN)
        catalog, schema = AthenaDataSourceImpl._normalize_athena_prefixes(None, identifier.prefixes)

        namespace = DbSchemaDataSourceNamespace(database=catalog, schema=schema)
        dialect = AthenaSqlDialect()

        # information_schema_namespace_elements produces the FROM prefix
        ns_elements = dialect.information_schema_namespace_elements(namespace)
        assert ns_elements == [S3_TABLES_CATALOG, "information_schema"]

    def test_full_metadata_query_sql(self):
        """Full column metadata query SQL uses the correct catalog in FROM and WHERE."""
        identifier = DatasetIdentifier.parse(DQN)
        catalog, schema = AthenaDataSourceImpl._normalize_athena_prefixes(None, identifier.prefixes)

        namespace = DbSchemaDataSourceNamespace(database=catalog, schema=schema)
        dialect = AthenaSqlDialect()

        sql = dialect.build_columns_metadata_query_str(table_namespace=namespace, table_name=identifier.dataset_name)

        # FROM clause should reference the correct catalog
        assert '"s3tablescatalog/my_bucket"."information_schema"' in sql
        # WHERE clause should filter on the correct catalog
        assert "'s3tablescatalog/my_bucket'" in sql
        # WHERE clause should filter on the correct schema
        assert f"'{SCHEMA}'" in sql.lower()
        # WHERE clause should filter on the correct table
        assert f"'{TABLE}'" in sql.lower()


class TestE2EDDLFlowWithSlashCatalog:
    """Verify DDL quoting works correctly when catalog contains '/'."""

    def test_ddl_quoting_from_dml_name(self):
        """DDL conversion should correctly re-quote the 3-part name with backticks."""
        dialect = AthenaSqlDialect()

        # Build the DML-quoted name (using double quotes)
        dml_name = dialect._build_qualified_quoted_dataset_name(
            dataset_name=TABLE,
            dataset_prefix=["s3tablescatalog", "my_bucket", SCHEMA],
        )
        assert dml_name == f'"s3tablescatalog/my_bucket"."{SCHEMA}"."{TABLE}"'

        # Convert to DDL quoting (backticks for Athena)
        ddl_name = dialect._convert_fqn_for_ddl(dml_name)
        assert ddl_name == f"`s3tablescatalog/my_bucket`.`{SCHEMA}`.`{TABLE}`"


class TestE2ERoundTripWithSlashCatalog:
    """Verify that DQN round-trip (parse → to_string → parse) preserves the structure."""

    def test_roundtrip_preserves_table_name(self):
        identifier = DatasetIdentifier.parse(DQN)
        reconstructed = identifier.to_string()
        re_parsed = DatasetIdentifier.parse(reconstructed)

        # Table name and data source are always correct
        assert re_parsed.dataset_name == TABLE
        assert re_parsed.data_source_name == "athena_ds"

    def test_roundtrip_athena_normalization_recovers_catalog(self):
        """Even after round-trip, Athena normalization recovers the correct catalog."""
        identifier = DatasetIdentifier.parse(DQN)
        reconstructed = identifier.to_string()
        re_parsed = DatasetIdentifier.parse(reconstructed)

        catalog, schema = AthenaDataSourceImpl._normalize_athena_prefixes(None, re_parsed.prefixes)
        assert catalog == S3_TABLES_CATALOG
        assert schema == SCHEMA


class TestBackwardCompatibilityRegularCatalog:
    """Ensure regular Athena catalogs (no '/') still work identically."""

    REGULAR_DQN = "athena_ds/awsdatacatalog/my_schema/my_table"

    def test_regular_dqn_parsing(self):
        identifier = DatasetIdentifier.parse(self.REGULAR_DQN)
        assert identifier.prefixes == ["awsdatacatalog", "my_schema"]

    def test_regular_from_clause(self):
        identifier = DatasetIdentifier.parse(self.REGULAR_DQN)
        dialect = AthenaSqlDialect()
        sql = dialect.build_select_sql(
            [
                SELECT(STAR()),
                FROM(identifier.dataset_name, identifier.prefixes),
            ]
        )
        assert '"awsdatacatalog"."my_schema"."my_table"' in sql

    def test_regular_prefix_extraction(self):
        identifier = DatasetIdentifier.parse(self.REGULAR_DQN)
        catalog, schema = AthenaDataSourceImpl._normalize_athena_prefixes(None, identifier.prefixes)
        assert catalog == "awsdatacatalog"
        assert schema == "my_schema"

    def test_regular_metadata_query(self):
        identifier = DatasetIdentifier.parse(self.REGULAR_DQN)
        catalog, schema = AthenaDataSourceImpl._normalize_athena_prefixes(None, identifier.prefixes)
        namespace = DbSchemaDataSourceNamespace(database=catalog, schema=schema)
        dialect = AthenaSqlDialect()
        sql = dialect.build_columns_metadata_query_str(table_namespace=namespace, table_name=identifier.dataset_name)
        assert '"awsdatacatalog"."information_schema"' in sql
        assert "'awsdatacatalog'" in sql
