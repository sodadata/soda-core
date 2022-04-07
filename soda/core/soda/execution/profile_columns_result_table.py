from typing import Dict, List

from soda.execution.profile_columns_result_column import ProfileColumnsResultColumn


class ProfileColumnsResultTable:

    def __init__(self, table_name: str, row_count: int):
        self.table_name: str = table_name
        self.row_count: int = row_count
        self.result_columns: List[ProfileColumnsResultColumn] = []

    def create_column(self, column_name: str, column_type: str) -> ProfileColumnsResultColumn:
        column = ProfileColumnsResultColumn(column_name, column_type)
        self.result_columns.append(column)
        return column

    def get_cloud_dict(self) -> dict:
        cloud_dict = {
            "tableName": self.table_name,
            "columnProfiles": [
                result_column.get_cloud_dict() for result_column in self.result_columns
            ]
        }
        return cloud_dict
