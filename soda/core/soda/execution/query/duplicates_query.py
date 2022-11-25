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
        if self.partition.sql_partition_filter:
            values_filter_clauses.append(self.partition.sql_partition_filter)

        values_filter = " \n  AND ".join(values_filter_clauses)

        column_names = ", ".join(self.metric.metric_args)

        # This does not respect the exclude_columns config because removing any of the excluded columns here would
        # effectively change the definition of the check. Let all columns through and samples will not be collected
        # if excluded columns are present (see "gatekeeper" in Query).
        data_source = self.data_source_scan.data_source
        table_name = self.partition.table.qualified_table_name

        self.sql = data_source.sql_get_duplicates(column_names, table_name, values_filter)
        self.passing_sql = data_source.sql_get_duplicates(
            column_names, table_name, values_filter, invert_condition=True
        )

    def execute(self):
        self.store()
        if self.sample_ref:
            self.metric.set_value(self.sample_ref.total_row_count)
            if self.sample_ref.is_persisted():
                self.metric.failed_rows_sample_ref = self.sample_ref
