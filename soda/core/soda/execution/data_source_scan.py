from typing import TYPE_CHECKING, Dict, List

from soda.execution.data_source import DataSource
from soda.execution.discover_tables_run import DiscoverTablesRun
from soda.execution.metric import Metric
from soda.execution.profile_columns_run import ProfileColumnsRun
from soda.execution.query import Query
from soda.execution.sample_tables_run import SampleTablesRun
from soda.execution.table import Table
from soda.sodacl.data_source_scan_cfg import DataSourceScanCfg

if TYPE_CHECKING:
    from soda.scan import Scan


class DataSourceScan:
    def __init__(
        self,
        scan: "Scan",
        data_source_scan_cfg: DataSourceScanCfg,
        data_source: DataSource,
    ):
        from soda.execution.metric import Metric
        from soda.execution.table import Table

        self.scan: Scan = scan
        self.data_source_scan_cfg: DataSourceScanCfg = data_source_scan_cfg
        self.metrics: List[Metric] = []
        self.data_source: DataSource = data_source
        self.tables: Dict[str, Table] = {}
        self.queries: List[Query] = []

    def get_or_create_table(self, table_name: str) -> Table:
        table = self.tables.get(table_name)
        if table is None:
            table = Table(self, table_name)
            self.tables[table_name] = table
        return table

    def resolve_metric(self, metric: "Metric") -> Metric:
        """
        If the metric is not added before, this method will:
         - Add the metric to scan.metrics
         - Ensure the metric is added to the appropriate query (if applicable)
        """
        existing_metric = self.scan._find_existing_metric(metric)
        if existing_metric:
            existing_metric.merge_checks(metric)
            return existing_metric
        self.scan._add_metric(metric)
        metric.ensure_query()
        return metric

    def get_queries(self):
        return

    def execute_queries(self):
        all_data_source_queries: List[Query] = []
        for table in self.tables.values():
            for partition in table.partitions.values():
                partition_queries = partition.collect_queries()
                all_data_source_queries.extend(partition_queries)
        all_data_source_queries.extend(self.queries)

        for query in all_data_source_queries:
            query.execute()

    def create_automated_monitor_run(self, automated_monitoring_cfg, scan):
        from soda.execution.automated_monitoring_run import AutomatedMonitoringRun

        return AutomatedMonitoringRun(self, automated_monitoring_cfg)

    def create_profile_columns_run(self, profile_columns_cfg, scan):
        return ProfileColumnsRun(self, profile_columns_cfg)

    def create_discover_tables_run(self, data_source_check_cfg, scan):
        return DiscoverTablesRun(self, data_source_check_cfg)

    def create_sample_tables_run(self, data_source_check_cfg):
        return SampleTablesRun(self, data_source_check_cfg)
