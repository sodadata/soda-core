from soda.execution.query.schema_query import TableColumnsQuery


class SparkTableColumnsQuery(TableColumnsQuery):
    def __init__(self, partition: "Partition", schema_metric: "SchemaMetric"):
        super().__init__(partition=partition, schema_metric=schema_metric)
        self.metric = schema_metric

    def _initialize_column_rows(self):
        """
        Initializes member self.rows as a list (or tuple) of rows where each row representing a column description.
        A column description is a list (or tuple) of column name on index 0 and column data type (str) on index 1
        Eg [["col_name_one", "data_type_of_col_name_one"], ...]
        """
        data_source = self.data_source_scan.data_source
        table_df = data_source.spark_session.table(self.table.table_name)
        self.rows = tuple([field.name, field.simpleString()] for field in table_df.schema.fields)
        self.row_count = len(self.rows)
        self.description = (("col_name", "StringType"), ("data_type", "StringType"))
