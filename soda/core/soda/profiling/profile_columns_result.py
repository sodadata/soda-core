from typing import List

from soda.profiling.profile_columns_result_table import ProfileColumnsResultTable
from soda.sodacl.profile_columns_cfg import ProfileColumnsCfg


class ProfileColumnsResult:
    def __init__(self, profile_columns_cfg: ProfileColumnsCfg):
        self.profile_columns_cfg: ProfileColumnsCfg = profile_columns_cfg
        self.tables: List[ProfileColumnsResultTable] = []

    def create_table(self, table_name: str, data_source_name: str, row_count: int) -> ProfileColumnsResultTable:
        table = ProfileColumnsResultTable(table_name, data_source_name, row_count)
        self.tables.append(table)
        return table
