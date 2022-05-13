from __future__ import annotations


class ProfileColumnsResultColumn:
    def __init__(self, column_name: str, column_type: str):
        self.column_name: str = column_name
        self.mins: list[float | int] | None = None
        self.maxes: list[float | int] | None = None
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

    def get_cloud_dict(self) -> dict:
        cloud_dict = {
            "columnName": self.column_name,
            "profile": {
                "mins": self.mins,
                "maxs": self.maxes,
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
