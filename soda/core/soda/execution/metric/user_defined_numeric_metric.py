from __future__ import annotations

from numbers import Number

from soda.execution.metric.query_metric import QueryMetric
from soda.execution.query.sample_query import SampleQuery
from soda.execution.query.user_defined_numeric_query import UserDefinedNumericQuery


class UserDefinedNumericMetric(QueryMetric):
    def __init__(
        self,
        data_source_scan: DataSourceScan,
        check_name: str,
        sql: str,
        check: Check = None,
    ):
        super().__init__(
            data_source_scan=data_source_scan,
            partition=None,
            column=None,
            name=check_name,
            check=check,
            identity_parts=[sql],
        )
        self.sql = sql
        self.check = check

    def __str__(self):
        return f'"{self.name}"'

    def set_value(self, value):
        if value is None or isinstance(value, float):
            self.value = value
        else:
            self.value = float(value)

    def ensure_query(self):
        query = UserDefinedNumericQuery(
            data_source_scan=self.data_source_scan,
            check_name=self.name,
            sql=self.sql,
            metric=self,
        )
        self.queries.append(query)
        self.data_source_scan.queries.append(query)

    def create_failed_rows_sample_query(self) -> SampleQuery | None:
        sampler = self.data_source_scan.scan._configuration.sampler
        if sampler and isinstance(self.value, Number) and self.check.check_cfg.failed_rows_query:
            if self.samples_limit > 0:
                jinja_resolve = self.data_source_scan.scan.jinja_resolve
                sql = jinja_resolve(self.check.check_cfg.failed_rows_query)
                sample_query = SampleQuery(self.data_source_scan, self, "failed_rows", sql)

                return sample_query
