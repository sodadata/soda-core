import logging
from typing import List, Optional

from soda.execution.data_source import DataSource

logger = logging.getLogger(__name__)


class DataSourceImpl(DataSource):
    def connect(self, connection_properties):
        import psycopg2

        schema = self.data_source_properties.get("schema")
        options = f"-c search_path={schema}" if schema else None

        password = connection_properties.get("password")
        if password == "":
            password = None

        host = connection_properties.get("host")
        if isinstance(host, str) and len(host) > 0:
            self.connection = psycopg2.connect(
                user=connection_properties.get("username"),
                password=password,
                host=host,
                port=connection_properties.get("port"),
                connect_timeout=connection_properties.get("connection_timeout"),
                database=self.data_source_properties.get("database"),
                options=options,
            )
        else:
            raise ConnectionError(f"Invalid postgres connection properties: invalid host: {host}")
        return self.connection

    def expr_regexp_like(self, expr: str, excaped_regex_pattern: str):
        return f"{expr} ~ '{excaped_regex_pattern}'"

    def get_metric_sql_aggregation_expression(self, metric_name: str, metric_args: Optional[List[object]], expr: str):
        # TODO add all of these postgres specific statistical aggregate functions: https://www.postgresql.org/docs/9.6/functions-aggregate.html
        if metric_name in [
            "stddev",
            "stddev_pop",
            "stddev_samp",
            "variance",
            "var_pop",
            "var_samp",
        ]:
            return f"{metric_name.upper()}({expr})"
        if metric_name in ["percentile", "percentile_disc"]:
            # TODO ensure proper error if the metric_args[0] is not a valid number
            percentile_fraction = metric_args[1] if metric_args else None
            return f"PERCENTILE_DISC({percentile_fraction}) WITHIN GROUP (ORDER BY {expr})"
        return super().get_metric_sql_aggregation_expression(metric_name, metric_args, expr)

    @staticmethod
    def format_column_default(identifier: str) -> str:
        return identifier.lower()

    @staticmethod
    def format_type_default(identifier: str) -> str:
        return identifier.lower()

    def safe_connection_data(self):
        return [
            self.type,
            self.connection_properties.get("host"),
            self.connection_properties.get("port"),
            self.connection_properties.get("database"),
        ]
