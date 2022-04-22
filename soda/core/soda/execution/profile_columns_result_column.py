from __future__ import annotations


class ProfileColumnsResultColumn:
    def __init__(self, column_name: str, column_type: str):
        self.column_name: str = column_name
        self.mins: list[float | int] | None = None
        self.maxes: list[float | int] | None = None
        self.min: float | int | None = None
        self.max: float | int | None = None
        self.frequent_values: list[float | int | str] | None = None
        self.frequency: list[int] | None = None
        self.average: float | None = None
        self.sum: float | int | None = None
        self.standard_deviation: float | None = None
        self.variance: float | None = None

    def create_column(self, column_name):
        pass

    def get_cloud_dict(self) -> dict:
        cloud_dict = {"columnName": self.column_name, "profile": {"mins": self.mins}}
        return cloud_dict
