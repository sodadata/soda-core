from soda.execution.query.query import Query
from soda.execution.query.sample_query import SampleQuery


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

        self.samples_limit = self.metric.samples_limit

        values_filter_clauses = [f"{column_name} IS NOT NULL" for column_name in self.metric.metric_args]
        partition_filter = self.partition.sql_partition_filter
        if partition_filter:
            scan = self.data_source_scan.scan
            resolved_partition_filter = scan.jinja_resolve(definition=partition_filter)
            values_filter_clauses.append(resolved_partition_filter)

        if metric.filter:
            values_filter_clauses.append(metric.filter)

        values_filter = " \n  AND ".join(values_filter_clauses)

        column_names = ", ".join(self.metric.metric_args)

        table_name = self.partition.table.table_name
        exclude_patterns = self.data_source_scan.data_source.get_exclude_column_patterns_for_table(table_name)
        data_source = self.data_source_scan.data_source
        jinja_resolve = self.data_source_scan.scan.jinja_resolve

        self.sql = jinja_resolve(
            data_source.sql_get_duplicates_count(
                column_names,
                table_name,
                values_filter,
            )
        )

        self.failed_rows_sql = jinja_resolve(
            data_source.sql_get_duplicates(
                column_names,
                table_name,
                values_filter,
                self.samples_limit,
            )
        )
        self.failing_sql = jinja_resolve(
            data_source.sql_get_duplicates(
                column_names,
                table_name,
                values_filter,
                None,
            )
        )

        self.passing_sql = jinja_resolve(
            data_source.sql_get_duplicates(
                column_names,
                table_name,
                values_filter,
                None,
                invert_condition=True,
            )
        )

        self.failing_rows_sql_aggregated = jinja_resolve(
            data_source.sql_get_duplicates_aggregated(
                column_names,
                table_name,
                values_filter,
                self.samples_limit,
                invert_condition=False,
                exclude_patterns=exclude_patterns,
            )
        )

    def execute(self):
        self.fetchone()
        if self.row:
            duplicates_count = self.row[0]
            self.metric.set_value(duplicates_count)

            if duplicates_count and self.samples_limit > 0:
                # TODO: Sample Query execute implicitly stores the failed rows file reference in the passed on metric.
                sample_query = SampleQuery(
                    self.data_source_scan,
                    self.metric,
                    "failed_rows",
                    self.failed_rows_sql,
                )
                sample_query.execute()

            # TODO: This should be a second failed rows file, refactor failed rows to support multiple files.
            if self.failing_rows_sql_aggregated and self.samples_limit > 0:
                aggregate_sample_query = Query(
                    self.data_source_scan,
                    self.partition.table,
                    self.partition,
                    unqualified_query_name=f"duplicate_count[{'-'.join(self.metric.metric_args)}].failed_rows.aggregated",
                    sql=self.failing_rows_sql_aggregated,
                    samples_limit=self.samples_limit,
                )
                aggregate_sample_query.execute()
                self.aggregated_failed_rows_data = aggregate_sample_query.rows
