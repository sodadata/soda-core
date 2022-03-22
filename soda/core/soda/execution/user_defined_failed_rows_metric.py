from soda.execution.query_metric import QueryMetric
from soda.execution.user_defined_failed_rows_query import UserDefinedFailedRowsQuery


class UserDefinedFailedRowsMetric(QueryMetric):
    def __init__(
        self,
        data_source_scan: "DataSourceScan",
        check_name: str,
        query: str,
        check: "Check",
    ):
        super().__init__(
            data_source_scan=data_source_scan,
            partition=None,
            column=None,
            name=check_name,
            check=check,
            identity_parts=[query],
        )

        self.query: str = query

    def __str__(self):
        return f'"{self.name}"'

    def set_value(self, value):
        if value is None or isinstance(value, int):
            self.value = value
        else:
            self.value = int(value)

    def ensure_query(self):
        self.data_source_scan.queries.append(
            UserDefinedFailedRowsQuery(data_source_scan=self.data_source_scan, metric=self)
        )
