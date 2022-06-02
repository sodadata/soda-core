from pyspark.sql import SparkSession

from soda.execution.schema_query import SchemaQuery


class SparkDfSchemaQuery(SchemaQuery):

    def __init__(self, partition: "Partition", schema_metric: "SchemaMetric"):
        super().__init__(partition, schema_metric)

    def _initialize_column_rows(self):
        self.rows = None
        spark_session: SparkSession = self.data_source_scan.data_source.connection.spark_session
        spark_table = spark_session.table(self.table.table_name)
        if spark_table:
            self.rows = []
            spark_table_schema = spark_table.schema
            for field in spark_table_schema.fields:
                column_row = [field.name, field.dataType.simpleString()]
                self.rows.append(column_row)
