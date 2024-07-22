from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row


class SparkDfCursor:
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
        self.df: DataFrame | None = None
        self.description: tuple[tuple] | None = None
        self.rowcount: int = -1
        self.cursor_index: int = -1

    def execute(self, sql: str):
        self.df = self.spark_session.sql(sqlQuery=sql)
        self.description = self.convert_spark_df_schema_to_dbapi_description(self.df)
        self.cursor_index = 0

    def fetchall(self) -> tuple[tuple]:
        rows = []
        spark_rows: list[Row] = self.df.collect()
        self.rowcount = len(spark_rows)
        for spark_row in spark_rows:
            row = self.convert_spark_row_to_dbapi_row(spark_row)
            rows.append(row)
        return tuple(rows)

    def fetchmany(self, size: int) -> tuple[tuple]:
        rows = []
        self.rowcount = self.df.count()
        spark_rows: list[Row] = self.df.limit(size).offset(self.cursor_index).collect()
        self.cursor_index += len(spark_rows)
        for spark_row in spark_rows:
            row = self.convert_spark_row_to_dbapi_row(spark_row)
            rows.append(row)
        return tuple(rows)

    def fetchone(self) -> tuple:
        spark_rows: list[Row] = self.df.collect()
        self.rowcount = len(spark_rows)
        spark_row = spark_rows[0]
        row = self.convert_spark_row_to_dbapi_row(spark_row)
        return tuple(row)

    @staticmethod
    def convert_spark_row_to_dbapi_row(spark_row):
        return [spark_row[field] for field in spark_row.__fields__]

    def close(self):
        pass

    @staticmethod
    def convert_spark_df_schema_to_dbapi_description(df) -> tuple[tuple]:
        return tuple((field.name, type(field.dataType).__name__) for field in df.schema.fields)
