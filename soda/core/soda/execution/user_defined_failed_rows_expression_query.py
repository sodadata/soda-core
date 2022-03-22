from soda.execution.query import Query


class UserDefinedFailedRowsExpressionQuery(Query):
    def __init__(self, data_source_scan: "DataSourceScan", check_name: str, sql: str):
        super().__init__(
            data_source_scan=data_source_scan,
            unqualified_query_name=f"user_defined_failed_rows_expression_query[{check_name}]",
        )
        self.sql: str = sql

    def execute(self):
        self.store()
