from soda.execution.query.query import Query


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

        # TODO: refactor this, it is confusing that samples limits are coming from different places.
        self.samples_limit = self.metric.samples_limit

        values_filter_clauses = [f"{column_name} IS NOT NULL" for column_name in self.metric.metric_args]
        partition_filter = self.partition.sql_partition_filter
        if partition_filter:
            scan = self.data_source_scan.scan
            resolved_partition_filter = scan.jinja_resolve(definition=partition_filter)
            values_filter_clauses.append(resolved_partition_filter)

        values_filter = " \n  AND ".join(values_filter_clauses)

        column_names = ", ".join(self.metric.metric_args)

        self.sql = self.data_source_scan.scan.jinja_resolve(
            self.data_source_scan.data_source.sql_get_duplicates(
                column_names, self.partition.table.qualified_table_name, values_filter
            )
        )

    def execute(self):
        self.store()
        if self.sample_ref:
            self.metric.set_value(self.sample_ref.total_row_count)
            if self.sample_ref.is_persisted():
                self.metric.failed_rows_sample_ref = self.sample_ref
