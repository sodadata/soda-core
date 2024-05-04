from __future__ import annotations

from soda.execution.query.query import Query

from soda.core.soda.execution.query.sample_query import SampleQuery


class UserDefinedFailedRowsExpressionQuery(Query):
    def __init__(
        self,
        data_source_scan: DataSourceScan,
        check_name: str,
        sql: str,
        partition: Partition,
        samples_limit: int | None = None,
        metric: Metric = None,
    ):
        super().__init__(
            data_source_scan=data_source_scan,
            sql=sql,
            unqualified_query_name=f"user_defined_failed_rows_expression_query[{check_name}]",
            partition=partition,
        )
        self.samples_limit = samples_limit
        self.metric = metric

    def execute(self):
        # By default don't store samples through the store method to circumvent sample limit not being applied to
        # failed row checks
        self.store(allow_samples=False)

        samples_sql = self.sql
        if self.metric.samples_limit:
            samples_sql += f"\nLIMIT {self.metric.samples_limit}"
        sample_query = SampleQuery(self.data_source_scan, self.metric, "failed_rows", samples_sql)
        sample_query.execute()
