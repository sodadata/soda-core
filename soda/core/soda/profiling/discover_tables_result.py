from typing import List

from soda.profiling.discover_table_result_table import DiscoverTablesResultTable
from soda.sodacl.data_source_tables_cfg import DatasourceTablesCfg


class DiscoverTablesResult:
    def __init__(self, data_source_tables_cfg: DatasourceTablesCfg):
        self.data_source_tables_cfg: DatasourceTablesCfg = data_source_tables_cfg
        self.tables: List[DiscoverTablesResultTable] = []

    def create_table(self, table_name: str, data_source_name: str, row_count: int) -> DiscoverTablesResultTable:
        table = DiscoverTablesResultTable(table_name, data_source_name, row_count)
        self.tables.append(table)
        return table
