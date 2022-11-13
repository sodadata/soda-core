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
        # Some queries doesn't work with capital letters in the column names
        lower_sql = sql.lower()

        # Run sql query in dask sql context and replace np.nan with None
        self.df = self.context.sql(lower_sql).compute().replace({np.nan: None})
        self.description: tuple = self.get_description()

    def fetchall(self) -> tuple[list, ...]:
        self.row_count = self.df.shape[0]
        rows: tuple[list, ...] = tuple(self.df.values.tolist())
        return rows

    def fetchone(self) -> tuple:
        self.row_count = self.df.shape[0]
        row_value = self.df.values[0]
        return tuple(row_value)

    def close(self) -> None:
        ...

    def get_description(self) -> tuple:
        if self.df.empty:
            return tuple((column, None) for column in self.df.columns)
        return tuple((column, type(self.df[column][0]).__name__) for column in self.df.columns)
