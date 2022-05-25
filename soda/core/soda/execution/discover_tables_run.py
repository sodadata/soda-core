from __future__ import annotations

from typing import TYPE_CHECKING

from soda.execution.query import Query
from soda.profiling.discover_tables_result import DiscoverTablesResult
from soda.sodacl.data_source_check_cfg import DataSourceCheckCfg

if TYPE_CHECKING:
    from soda.execution.data_source_scan import DataSourceScan


QUERY_PREFIX = "discover_tables: "


class DiscoverTablesRun:
    def __init__(self, data_source_scan: DataSourceScan, data_source_check_cfg: DataSourceCheckCfg):

        self.data_source_scan = data_source_scan
        self.soda_cloud = data_source_scan.scan._configuration.soda_cloud
        self.data_source = data_source_scan.data_source
        self.data_source_check_cfg: DataSourceCheckCfg = data_source_check_cfg
        self.logs = self.data_source_scan.scan._logs

    def run(self) -> DiscoverTablesResult:
        discover_tables_result: DiscoverTablesResult = DiscoverTablesResult(self.data_source_check_cfg)
        self.logs.info(f"Running discover tables for data source: {self.data_source.data_source_name}")

        # row_counts is a dict that maps table names to row counts.
        row_counts_by_table_name: dict[str, int] = self.data_source.get_row_counts_all_tables(
            include_tables=self.data_source_check_cfg.include_tables,
            exclude_tables=self.data_source_check_cfg.exclude_tables,
            query_name=f"{QUERY_PREFIX} get tables and row counts",
        )
        for table_name in row_counts_by_table_name:
            self.logs.debug(f"Discovering columns for {table_name}")
            measured_row_count = row_counts_by_table_name[table_name]
            discover_tables_result_table = discover_tables_result.create_table(
                table_name, self.data_source.data_source_name, measured_row_count
            )

            # get columns & metadata for current table
            columns_metadata_sql = self.data_source.sql_to_get_column_metadata_for_table(table_name)
            columns_metadata_query = Query(
                data_source_scan=self.data_source_scan,
                unqualified_query_name=f"{QUERY_PREFIX} get col metadata for table: {table_name}",
                sql=columns_metadata_sql,
            )
            columns_metadata_query.execute()
            assert columns_metadata_query.rows, f"No metadata was captured for table: {table_name}"
            columns_metadata_result = {column[0]: column[1] for column in columns_metadata_query.rows}
            for column_name, column_type in columns_metadata_result.items():
                _ = discover_tables_result_table.create_column(column_name, column_type)

        if not discover_tables_result.tables:
            self.logs.error(f"Discover tables for data source: {self.data_source.data_source_name} failed")
        return discover_tables_result
