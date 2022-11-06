from __future__ import annotations

from dask import dataframe
from dask_sql import Context


class DaskCursor:
    def __init__(self, context: Context):
        self.context = context
        self.df: dataframe | None = None
        self.description: tuple[tuple] | None = None
        self.row_count: int = -1

    def execute(self, sql: str) -> None:
        self.df = self.context.sql(sql).compute()
        self.description = self.get_description()

    def fetchall(self) -> tuple[tuple]:
        ...

    def fetchone(self) -> tuple:
        return tuple(self.df.values[0])

    def close(self):
        pass

    def get_description(self) -> tuple[tuple]:
        return tuple((field, type(self.df[field][0]).__name__) for field in self.df.columns)
