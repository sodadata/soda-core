from soda.execution.query import Query


class DuplicatesQuery(Query):
    def __init__(self, partition: "Partition", metric: "Metric"):
        super().__init__(
            data_source_scan=partition.data_source_scan,
            table=partition.table,
            partition=partition,
            column=metric.column,
            unqualified_query_name=f"duplicate_count",
        )
        self.metric = metric

        values_filter_clauses = [f"{column_name} IS NOT NULL" for column_name in self.metric.metric_args]
        partition_filter = self.partition.sql_partition_filter
        if partition_filter:
            scan = self.data_source_scan.scan
            location = self.metric._checks[0].location if self.metric._checks else None
            resolved_partition_filter = scan._jinja_resolve(definition=partition_filter, location=location)
            values_filter_clauses.append(resolved_partition_filter)

        values_filter = " \n  AND ".join(values_filter_clauses)

        column_names = ", ".join(self.metric.metric_args)

        self.sql = (
            f"WITH frequencies AS (\n"
            f"  SELECT {column_names}, COUNT(*) AS frequency \n"
            f"  FROM {self.partition.table.prefixed_table_name} \n"
            f"  WHERE {values_filter} \n"
            f"  GROUP BY {column_names}) \n"
            f"SELECT * \n"
            f"FROM frequencies \n"
            f"WHERE frequency > 1;"
        )

    def execute(self):
        self.store()
        if self.storage_ref:
            values_having_duplicates = self.storage_ref.total_row_count
            self.metric.set_value(values_having_duplicates)
            self.metric.duplicate_frequencies_storage_ref = self.storage_ref
