from __future__ import annotations

from numbers import Number
from typing import Any

from soda.sodacl.data_source_check_cfg import ProfileColumnsCfg


class ProfileColumnsResultColumn:
    def __init__(self, column_name: str, column_type: str):
        self.column_name: str = column_name
        self.column_type: str = column_type
        self.mins: list[float | int] | None = None
        self.maxs: list[float | int] | None = None
        self.min: float | int | None = None
        self.max: float | int | None = None
        self.frequent_values: list[dict] | None = None
        self.average: float | None = None
        self.sum: float | int | None = None
        self.standard_deviation: float | None = None
        self.variance: float | None = None
        self.distinct_values: int | None = None
        self.missing_values: int | None = None
        self.histogram: dict[str, list[str | int | float]] | None = None
        self.average_length: float | None = None
        self.min_length: float | None = None
        self.max_length: float | None = None

    def set_mins(self, value_frequencies: list[tuple]) -> None:
        self.mins = [self.unify_type(row[2]) for row in value_frequencies if row[0] == "mins"]

    def set_maxs(self, value_frequencies: list[tuple]) -> None:
        self.maxs = [self.unify_type(row[2]) for row in value_frequencies if row[0] == "maxs"]

    def set_min(self) -> None:
        if self.mins:
            self.min = self.mins[0]

    def set_max(self) -> None:
        if self.maxs:
            self.max = self.maxs[0]

    def set_frequent_values(self, value_frequencies: list[tuple]) -> None:
        self.frequent_values = [
            {"value": str(row[2]), "count": int(row[3])} for row in value_frequencies if row[0] == "frequent_values"
        ]

    @staticmethod
    def unify_type(v: Any) -> Any:
        if isinstance(v, Number):
            return float(v)
        else:
            return v

    def get_cloud_dict(self) -> dict:
        cloud_dict = {
            "columnName": self.column_name,
            "profile": {
                "mins": self.mins,
                "maxs": self.maxs,
                "min": self.min,
                "max": self.max,
                "frequent_values": self.frequent_values,
                "avg": self.average,
                "sum": self.sum,
                "stddev": self.standard_deviation,
                "variance": self.variance,
                "distinct": self.distinct_values,
                "missing_count": self.missing_values,
                "histogram": self.histogram,
                "avg_length": self.average_length,
                "min_length": self.min_length,
                "max_length": self.max_length,
            },
        }
        return cloud_dict

    def get_dict(self) -> dict:
        return {
            "columnName": self.column_name,
            "profile": {
                "mins": self.mins,
                "maxs": self.maxs,
                "min": self.min,
                "max": self.max,
                "frequent_values": self.frequent_values,
                "avg": self.average,
                "sum": self.sum,
                "stddev": self.standard_deviation,
                "variance": self.variance,
                "distinct": self.distinct_values,
                "missing_count": self.missing_values,
                "histogram": self.histogram,
                "avg_length": self.average_length,
                "min_length": self.min_length,
                "max_length": self.max_length,
            },
        }


class ProfileColumnsResultTable:
    def __init__(self, table_name: str, data_source: str, row_count: int | None = None):
        self.table_name: str = table_name
        self.data_source: str = data_source
        self.row_count: int | None = row_count
        self.result_columns: list[ProfileColumnsResultColumn] = []

    def create_column(self, column_name: str, column_type: str) -> ProfileColumnsResultColumn:
        column = ProfileColumnsResultColumn(column_name, column_type)
        self.result_columns.append(column)
        return column

    def get_cloud_dict(self) -> dict:
        cloud_dict = {
            "table": self.table_name,
            "dataSource": self.data_source,
            "rowCount": self.row_count,
            "columnProfiles": [result_column.get_cloud_dict() for result_column in self.result_columns],
        }
        return cloud_dict

    def get_dict(self) -> dict:
        return {
            "table": self.table_name,
            "dataSource": self.data_source,
            "rowCount": self.row_count,
            "columnProfiles": [result_column.get_dict() for result_column in self.result_columns],
        }


class ProfileColumnsResult:
    def __init__(self, profile_columns_cfg: ProfileColumnsCfg):
        self.profile_columns_cfg: ProfileColumnsCfg = profile_columns_cfg
        self.tables: list[ProfileColumnsResultTable] = []

    def create_table(
        self, table_name: str, data_source_name: str, row_count: int | None = None
    ) -> ProfileColumnsResultTable:
        table = ProfileColumnsResultTable(table_name, data_source_name, row_count)
        self.tables.append(table)
        return table
