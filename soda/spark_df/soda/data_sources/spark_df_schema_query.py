from pyspark.sql import SparkSession
from soda.execution.schema_query import TableColumnsQuery


class SparkDfTableColumnsQuery(TableColumnsQuery):
    def __init__(self, partition: "Partition", schema_metric: "SchemaMetric"):
        super().__init__(partition, schema_metric)

    def _initialize_column_rows(self):
        data_source = self.data_source_scan.data_source
        self.rows = data_source.get_table_column_rows(self.table.table_name)
