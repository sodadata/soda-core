from soda.execution.query import Query


class UserDefinedFailedRowsQuery(Query):
    def __init__(
        self,
        data_source_scan: "DataSourceScan",
        metric: "FailedRowsQueryMetric",
    ):
        super().__init__(
            data_source_scan=data_source_scan,
            unqualified_query_name=f"failed_rows[{metric.name}]",
        )
        self.sql = metric.query
        self.metric = metric

    def execute(self):
        self.store()
        if self.storage_ref:
            self.metric.value = self.storage_ref.total_row_count
            self.metric.failed_rows_storage_ref = self.storage_ref
