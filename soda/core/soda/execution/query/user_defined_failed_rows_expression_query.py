from __future__ import annotations

from soda.execution.query.query import Query


class UserDefinedFailedRowsExpressionQuery(Query):
    def __init__(self, data_source_scan: DataSourceScan, check_name: str, sql: str, samples_limit: int | None = 100):
        super().__init__(
            data_source_scan=data_source_scan,
            unqualified_query_name=f"user_defined_failed_rows_expression_query[{check_name}]",
        )
        self.sql: str = sql
        self.samples_limit = samples_limit

    def execute(self):
        self.store()
