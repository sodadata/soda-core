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
        sql = self._handle_uppercase_queries(sql)
        self.df = self.context.sql(sql).compute().replace({np.nan: None})

        # Reset index
        self.df = self.df.reset_index(drop=True)
        self.description: tuple = self.get_description()

    def fetchall(self) -> tuple[list, ...]:
        self.row_count = self.df.shape[0]
        rows: tuple[list, ...] = tuple(self.df.values.tolist())
        return rows

    def fetchone(self) -> tuple:
        self.row_count = self.df.shape[0]
        if self.df.empty:
            row_value = []
            for col_dtype in self.df.dtypes:
                if col_dtype in ["int", "float"]:
                    row_value.append(0)
                else:
                    row_value.append(None)
        else:
            row_value = self.df.values[0]
        return tuple(row_value)

    def close(self) -> None:
        ...

    def get_description(self) -> tuple:
        if self.df.empty:
            return tuple((column, None) for column in self.df.columns)
        return tuple((column, type(self.df[column][0]).__name__) for column in self.df.columns)

    @staticmethod
    def _handle_uppercase_queries(sql: str):
        """
        Due to a bug in dasksql the following uppercase identifiers
        does not work with dasksql. When the bug has been removed, then
        we can remove this temporary method

        e.g. select SOURCE.* from my_table SOURCE
        """
        return sql.replace("SOURCE", "source").replace("TARGET", "target")
