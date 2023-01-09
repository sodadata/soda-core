from __future__ import annotations
from collections import defaultdict
from typing import TYPE_CHECKING

from soda.profiling.discover_tables_result import DiscoverTablesResult
from soda.sodacl.data_source_check_cfg import DataSourceCheckCfg

if TYPE_CHECKING:
    from soda.execution.data_source_scan import DataSourceScan


class DiscoverTablesRun:
    def __init__(self, data_source_scan: DataSourceScan, data_source_check_cfg: DataSourceCheckCfg):

        self.data_source_scan = data_source_scan
        self.soda_cloud = data_source_scan.scan._configuration.soda_cloud
        self.data_source = data_source_scan.data_source
        self.data_source_check_cfg: DataSourceCheckCfg = data_source_check_cfg
        self.logs = self.data_source_scan.scan._logs

    def run(self) -> DiscoverTablesResult:
        discover_tables_result: DiscoverTablesResult = DiscoverTablesResult(self.data_source_check_cfg)
        self.logs.info(f"Running discover datasets for data source: {self.data_source.data_source_name}")

        include_patterns = [{"table_name_pattern": include_table} for include_table in self.data_source_check_cfg.include_tables]
        exclude_patterns = [{"table_name_pattern": exclude_table} for exclude_table in self.data_source_check_cfg.exclude_tables]
        # row_counts is a dict that maps table names to row counts.
        tables_columns_metadata: defaultdict[str, dict[str, str]] = self.data_source.get_tables_columns_metadata(
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            query_name="discover-tables-find-tables-and-row-counts"
        )

        if tables_columns_metadata is None:
            self.logs.warning(
                f"No table matching your SodaCL inclusion list found on your {self.data_source.data_source_name} "
                "data source. Table discovery results may be incomplete or entirely skipped",
                location=self.data_source_check_cfg.location,
            )
            return discover_tables_result

        self.logs.info(f"Discovering the following tables:")
        for table_name in tables_columns_metadata:
            self.logs.info(f"  - {table_name}")
            discover_tables_result_table = discover_tables_result.create_table(
                table_name, self.data_source.data_source_name
            )
            # get columns & metadata for current table
            columns_metadata_result = tables_columns_metadata.get(table_name)
            for column_name, column_type in columns_metadata_result.items():
                _ = discover_tables_result_table.create_column(column_name, column_type)

        return discover_tables_result
