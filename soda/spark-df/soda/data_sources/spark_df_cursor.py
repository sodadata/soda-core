from typing import Tuple

from pyspark.sql.dataframe import DataFrame


class SparkDfCursor:

    def __init__(self, df: DataFrame):
        self.df: DataFrame = df

    def fetchall(self) -> Tuple[Tuple]:
        rows = []
        return tuple(rows)
