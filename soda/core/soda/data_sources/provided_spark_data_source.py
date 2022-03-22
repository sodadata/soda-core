from typing import List

from soda.execution.data_source import DataSource


class ProvidedSparkSessionCursor:
    def __init__(self, connection):
        self.spark_session = connection.spark_session
        self.result_spark_dataframe = None
        self.description = None

    def execute(self, sql: str):
        self.result_spark_dataframe = self.spark_session.sql(sql)

    def fetchone(self):
        spark_rows: List["pyspark.sql.types.Row"] = self.result_spark_dataframe.collect()
        spark_row = spark_rows[0]
        values = [spark_row[field] for field in spark_row.__fields__]
        self.description = tuple((field, "?", True) for field in spark_row.__fields__)
        return tuple(values)

    def close(self):
        pass


class ProvidedSparkSessionConnection:
    def __init__(self, spark_session):
        self.spark_session = spark_session

    def cursor(self):
        return ProvidedSparkSessionCursor(self)

    def rollback(self):
        pass


class DataSourceImpl(DataSource):
    def connect(self, connection_properties: dict):
        provided_spark_session = connection_properties["provided_spark_session"]
        self.connection = ProvidedSparkSessionConnection(provided_spark_session)

    def validate_configuration(self, connection_properties: dict, logs: "Logs") -> None:
        pass
