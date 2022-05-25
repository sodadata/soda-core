from typing import Dict, List

from soda.execution.query import Query


class SchemaQuery(Query):
    def __init__(self, partition: "Partition", schema_metric: "SchemaMetric"):
        super().__init__(
            data_source_scan=partition.data_source_scan,
            table=partition.table,
            partition=partition,
            unqualified_query_name=f"schema[{partition.table.table_name}]",
        )
        data_source = self.data_source_scan.data_source
        self.metric = schema_metric
        self.sql = data_source.sql_to_get_column_metadata_for_table(self.table.table_name)

    def execute(self):
        self.fetchall()
        if len(self.rows) > 0:
            measured_schema: List[Dict[str, str]] = []
            for row in self.rows:
                measured_schema.append({"name": row[0], "type": row[1]})
            self.metric.set_value(measured_schema)
