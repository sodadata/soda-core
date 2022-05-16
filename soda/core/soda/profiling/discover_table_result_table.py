from typing import List

from soda.profiling.discover_tables_result_column import DiscoverTablesResultColumn


class DiscoverTablesResultTable:
    def __init__(self, table_name: str, data_source: str, row_count: int):
        self.table_name: str = table_name
        self.data_source: str = data_source
        self.row_count: int = row_count
        self.result_columns: List[DiscoverTablesResultColumn] = []

    def create_column(self, column_name: str, column_type: str) -> DiscoverTablesResultColumn:
        column = DiscoverTablesResultColumn(column_name, column_type)
        self.result_columns.append(column)
        return column

    def get_cloud_dict(self) -> dict:
        cloud_dict = {
            "table": self.table_name,
            "dataSource": self.data_source,
            "rowCount": self.row_count,
            "schema": [result_column.get_cloud_dict() for result_column in self.result_columns],
        }
        return cloud_dict
