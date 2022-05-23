from __future__ import annotations

from typing import TYPE_CHECKING, List

from soda.execution.query import Query
from soda.profiling.sample_tables_result import SampleTablesResult
from soda.sodacl.discover_tables_cfg import DiscoverTablesCfg

if TYPE_CHECKING:
    from soda.execution.data_source_scan import DataSourceScan


QUERY_PREFIX = "sample_tables: "


class SampleTablesRun:
    def __init__(self, data_source_scan: DataSourceScan, discover_tables_cfg: DiscoverTablesCfg):

        self.data_source_scan = data_source_scan
        self.soda_cloud = data_source_scan.scan._configuration.soda_cloud
        self.data_source = data_source_scan.data_source
        self.data_source_name = self.data_source.data_source_name
        self.discover_tables_cfg: DiscoverTablesCfg = discover_tables_cfg
        self.logs = self.data_source_scan.scan._logs

    def run(self) -> SampleTablesResult:
        sample_tables_result: SampleTablesResult = SampleTablesResult(self.discover_tables_cfg)
        self.logs.info(f"Running discover tables for data source: {self.data_source_name}")

        table_names: List[str] = self.data_source.get_table_names(
            include_tables=self.discover_tables_cfg.include_tables,
            exclude_tables=self.discover_tables_cfg.exclude_tables,
            query_name=f"{QUERY_PREFIX} get tables",
        )

        for table_name in table_names:
            self.logs.debug(f"Sampling columns for {table_name}")

            # get columns and first n rows
            sample_table_sql = self.data_source.sql_get_table(table_name, limit=100)
            sample_table_query = Query(
                data_source_scan=self.data_source_scan,
                unqualified_query_name=f"{QUERY_PREFIX} get samples from table: {table_name}",
                sql=sample_table_sql,
                sample_name="table_sample"
            )
            sample_table_query.store()
            sample_ref = sample_table_query.sample_ref

            _ = sample_tables_result.create_table(
                table_name, self.data_source_name, sample_ref
            )
            self.logs.info(f"Successfully collected samples for table: {table_name}!")

        if not sample_tables_result.tables:
            self.logs.error(f"Sample tables for data source: {self.data_source_name} failed")
            
        return sample_tables_result
