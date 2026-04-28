import logging
import re
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl, FullyQualifiedTableName
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_ast import FROM
from soda_core.common.sql_dialect import SqlDialect
from soda_odps.common.data_sources.odps_data_source_model import (
    OdpsDataSource as OdpsDataSourceModel,
)

logger: logging.Logger = soda_logger


class OdpsDataSourceImpl(DataSourceImpl, model_class=OdpsDataSourceModel):
    def __init__(self, data_source_model: OdpsDataSourceModel, connection: Optional[DataSourceConnection] = None):
        super().__init__(data_source_model=data_source_model, connection=connection)

    def _create_sql_dialect(self) -> SqlDialect:
        return OdpsSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        from soda_odps.common.data_sources.odps_data_source_connection import (
            OdpsDataSourceConnection,
        )

        return OdpsDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    def verify_if_table_exists(self, prefixes: list[str], table_name: str) -> bool:
        fully_qualified_table_names: list[FullyQualifiedTableName] = self._get_fully_qualified_table_names(
            prefixes=prefixes, table_name=table_name
        )
        return any(
            fully_qualified_table_name.table_name.lower() == table_name.lower()
            for fully_qualified_table_name in fully_qualified_table_names
        )


class OdpsSqlDialect(SqlDialect, sqlglot_dialect=None):
    """SQL dialect for ODPS (MaxCompute).

    MaxCompute uses a PostgreSQL-like SQL dialect with some extensions.
    Uses backticks for identifier quoting and rlike for regex matching.
    """

    DEFAULT_QUOTE_CHAR = "`"
    SUPPORTS_DROP_TABLE_CASCADE = False
    # Class variable to persist partition info across method calls
    _odps_partition: Optional[str] = None

    def __init__(self):
        """Initialize ODPS dialect, reset partition state."""
        super().__init__()
        # Reset partition for new query
        OdpsSqlDialect._odps_partition = None

    def quote_for_ddl(self, identifier: Optional[str]) -> Optional[str]:
        """MaxCompute DDL uses backticks."""
        return f"`{identifier}`" if isinstance(identifier, str) and len(identifier) > 0 else None

    def quote_default(self, identifier: Optional[str]) -> Optional[str]:
        """MaxCompute uses backticks for identifier quoting."""
        return f"`{identifier}`" if isinstance(identifier, str) and len(identifier) > 0 else None

    def quote_column(self, column_name: str) -> str:
        """MaxCompute uses backticks for column names."""
        return f"`{column_name}`"

    def supports_regex_advanced(self) -> bool:
        """ODPS/MaxCompute supports regex with rlike operator."""
        return True

    def _build_regex_like_sql(self, matches: "REGEX_LIKE") -> str:
        """ODPS uses rlike operator for regex matching instead of REGEXP_LIKE function."""
        expression: str = self.build_expression_sql(matches.expression)
        # Escape single quotes in the regex pattern by doubling them
        escaped_pattern = matches.regex_pattern.replace("'", "''")
        return f"{expression} RLIKE '{escaped_pattern}'"

    def _build_string_hash_sql(self, string_hash) -> str:
        """ODPS uses MD5 for string hashing."""
        return f"to_hex(md5(to_utf8({self.build_expression_sql(string_hash.expression)})))"

    def _build_columns_metadata_namespace(self, prefixes: list) -> "DataSourceNamespace":
        """Override to handle ODPS table naming."""
        from soda_core.common.sql_dialect import SchemaDataSourceNamespace

        # ODPS uses project.schema.table, where schema is the "project" in ODPS terms
        schema = None
        if prefixes:
            # prefixes[0] should be the ODPS project
            schema = prefixes[0]
        return SchemaDataSourceNamespace(schema=schema)

    def build_columns_metadata_query_str(self, table_namespace, table_name: str) -> str:
        """ODPS uses DESCRIBE TABLE instead of information_schema."""
        database_name = table_namespace.get_database_for_metadata_query()
        schema_name = table_namespace.get_schema_for_metadata_query()
        fully_qualified_name = self.qualify_dataset_name(
            dataset_prefix=[database_name, schema_name], dataset_name=table_name
        )
        return f"DESCRIBE {fully_qualified_name};"

    def _build_cte_sql_lines(self, select_elements: list) -> list[str]:
        """Build CTE with ODPS partition support.

        Override to extract partition filters from WHERE and add them to FROM PARTITION clause.
        """

        from soda_core.common.sql_dialect import CTE

        # Reset partition state at start
        OdpsSqlDialect._odps_partition = None

        # First, let parent build CTE normally
        cte_sql_lines = super()._build_cte_sql_lines(select_elements)

        # Find the CTE and its filter
        for select_element in select_elements:
            if isinstance(select_element, CTE) and select_element.filter:
                filter_str = select_element.filter
                logger.info(f"Found CTE filter: {filter_str}")
                # Check if this is a partition filter like "ds = '20260101'"
                partition_match = re.search(r"(\w+)\s*=\s*'([^']+)'", filter_str)
                if partition_match:
                    partition_col = partition_match.group(1)
                    partition_value = partition_match.group(2)
                    # Store partition info in class variable for use in FROM clause
                    OdpsSqlDialect._odps_partition = f"PARTITION({partition_col}='{partition_value}')"
                    logger.info(f"Extracted ODPS partition: {OdpsSqlDialect._odps_partition}")
                else:
                    logger.info(f"Filter is not a partition filter: {filter_str}")

        return cte_sql_lines

    def _build_from_part(self, from_part: FROM) -> str:
        """Build FROM clause with ODPS partition support.

        ODPS requires partition conditions in FROM clause using PARTITION clause,
        e.g., `table_name` PARTITION(ds='20260101')
        """
        from_parts: list[str] = [
            self._build_qualified_quoted_dataset_name(
                dataset_name=from_part.table_name, dataset_prefix=from_part.table_prefix
            )
        ]

        # Add partition from class variable if available
        if OdpsSqlDialect._odps_partition:
            from_parts.append(OdpsSqlDialect._odps_partition)

        if from_part.sampler_type is not None and isinstance(from_part.sample_size, Number):
            from_parts.append(self._build_sample_sql(from_part.sampler_type, from_part.sample_size))

        if isinstance(from_part.alias, str):
            from_parts.append(self._alias_format(from_part.alias))

        return " ".join(from_parts)

    def _build_from_sql_lines(self, select_elements: list) -> list[str]:
        """Build FROM clause - extract partition filter from CTE."""
        from soda_core.common.sql_dialect import CTE

        # First, build the basic FROM lines
        from_lines = super()._build_from_sql_lines(select_elements)

        # Debug: Find CTE and see if it has filter
        for select_element in select_elements:
            if isinstance(select_element, CTE):
                logger.info(f"Found CTE: {select_element.alias}")
                # CTE is a CTE object, let's see its contents
                logger.info(f"CTE content: {select_element}")
                # Check if CTE query is a list with filter
                if isinstance(select_element.cte_query, list):
                    for item in select_element.cte_query:
                        if item is not None:
                            logger.info(f"  CTE query item: {type(item).__name__} = {item}")

        return from_lines

    def _build_where_sql_lines(self, select_elements: list) -> list[str]:
        """Build WHERE clause, skipping partition filters that are handled in FROM PARTITION.

        Partition filters like "ds = '20260101'" are moved to FROM PARTITION clause in ODPS.
        """
        from soda_core.common.sql_ast import AND, WHERE

        and_expressions = []
        for select_element in select_elements:
            if isinstance(select_element, WHERE):
                and_expressions.append(select_element.condition)
            elif isinstance(select_element, AND):
                and_expressions.extend(select_element._get_clauses_as_list())

        # Filter out partition-only filters (they go to FROM PARTITION clause)
        filtered_expressions = []
        for expr in and_expressions:
            if expr:
                expr_str = self.build_expression_sql(expr)
                # Check if this is a pure partition filter like "`ds` = '20260101'" or "ds = '20260101'"
                if re.match(r"^`?\w+`?\s*=\s*'[^']+'$", expr_str):
                    logger.info(f"Skipping partition filter in WHERE: {expr_str}")
                    continue
                filtered_expressions.append(expr)

        where_parts = [self.build_expression_sql(and_expression) for and_expression in filtered_expressions]

        where_sql_lines = []
        for i in range(0, len(where_parts)):
            if i == 0:
                sql_line = f"WHERE {where_parts[0]}"
            else:
                sql_line = f"  AND {where_parts[i]}"
            where_sql_lines.append(sql_line)
        return where_sql_lines
