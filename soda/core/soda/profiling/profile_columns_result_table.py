from typing import List

from soda.profiling.profile_columns_result_column import ProfileColumnsResultColumn


class ProfileColumnsResultTable:
    def __init__(self, table_name: str, data_source: str, row_count: int):
        self.table_name: str = table_name
        self.data_source: str = data_source
        self.row_count: int = row_count
        self.result_columns: List[ProfileColumnsResultColumn] = []

    def create_column(self, column_name: str, column_type: str) -> ProfileColumnsResultColumn:
        column = ProfileColumnsResultColumn(column_name, column_type)
        self.result_columns.append(column)
        return column

    def get_cloud_dict(self) -> dict:
        cloud_dict = {
            "table": self.table_name,
            "dataSource": self.data_source,
            "columnProfiles": [result_column.get_cloud_dict() for result_column in self.result_columns],
        }
        return cloud_dict
