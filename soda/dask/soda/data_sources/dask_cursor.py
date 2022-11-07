from __future__ import annotations

import numpy as np
from dask.dataframe.core import DataFrame
from dask_sql import Context


class DaskCursor:
    def __init__(self, context: Context):
        self.context = context
        self.df: DataFrame
        self.description: tuple[tuple] | None = None
        self.row_count: int = -1

    def execute(self, sql: str) -> None:
        # Run sql query in dask sql context and replace np.nan with None
        self.df = self.context.sql(sql).compute().replace({np.nan: None})
        self.description = self.get_description()

    def fetchall(self) -> tuple[list]:
        self.row_count = self.df.shape[0]
        rows: list = self.df.values.tolist()
        return tuple(rows)

    def fetchone(self) -> tuple:
        self.row_count = self.df.shape[0]
        row_value = self.df.values[0]
        return tuple(row_value)

    def close(self):
        pass

    def get_description(self) -> tuple[tuple]:
        return tuple((field, type(self.df[field][0]).__name__) for field in self.df.columns)
