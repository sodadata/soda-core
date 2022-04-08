from textwrap import dedent
from typing import TYPE_CHECKING, Dict, List

from soda.execution.profile_columns_result import ProfileColumnsResult
from soda.execution.query import Query
from soda.sodacl.profile_columns_cfg import ProfileColumnsCfg

if TYPE_CHECKING:
    from soda.execution.data_source_scan import DataSourceScan


class ProfileColumnsRun:
    def __init__(self, data_source_scan: DataSourceScan, profile_columns_cfg: ProfileColumnsCfg):

        self.data_source_scan = data_source_scan
        self.soda_cloud = data_source_scan.scan._configuration.soda_cloud
        self.data_source = data_source_scan.data_source
        self.profile_columns_cfg: ProfileColumnsCfg = profile_columns_cfg
        self.logs = self.data_source_scan.scan._logs

    def run(self) -> ProfileColumnsResult:
        profile_columns_result: ProfileColumnsResult = ProfileColumnsResult(self.profile_columns_cfg)

        # row_counts is a dict that maps table names to row counts.
        row_counts_by_table_name: Dict[str, int] = self.data_source.get_row_counts_for_all_tables()
        for table_name in row_counts_by_table_name:
            measured_row_count = row_counts_by_table_name[table_name]
            profile_columns_result_table = profile_columns_result.create_table(table_name, measured_row_count)

            column_types_by_name = {"distance": "float"}

            for column_name, column_type in column_types_by_name.items():
                if self._is_column_included_for_profiling(column_name):
                    profile_columns_result_column = profile_columns_result_table.create_column(column_name, column_type)
                    sql = self.sql_values_frequencies_query(table_name, column_name)

                    query = Query(
                        data_source_scan=self.data_source_scan,
                        unqualified_query_name="get_profile_columns_metrics",
                        sql=sql,
                    )
                    query.execute()
                    profile_columns_result_column.mins = [row[0] for row in query.rows]

        return profile_columns_result

    def sql_values_frequencies_query(self, table_name: str, column_name: str) -> str:
        # with values as (
        #   select {colname} as value, count(*) as frequency
        #   from table [TABLESAMPLE ...]
        #   group by {colname}
        #   [where filter]
        # )
        # select 'mins', value, frequency
        # from values
        # order by value ASC
        # limit 10
        # UNION ALL
        # select 'maxs', value, frequency
        # from values
        # order by value DESC
        # limit 10
        # UNION ALL
        # select 'frequent values', value, frequency
        # from values
        # order by frequency DESC
        # limit 10
        # UNION ALL
        # select 'duplicates', value, frequency
        # from values
        # where frequency > 1
        # order by frequency DESC
        # limit 10
        return dedent(
            f"""
                WITH values AS (
                  {self.sql_cte_value_frequencies(table_name, column_name)}
                )
                {self.sql_mins()}
            """
        )

    def sql_cte_value_frequencies(self, table_name, column_name):
        return dedent(
            f"""
                SELECT {column_name} as value, count(*) as frequency
                FROM {table_name}
                GROUP BY value
            """
        )

    def sql_mins(self):
        return dedent(
            """
                 SELECT value, frequency, 'mins'
                 FROM values
                 ORDER BY value ASC
                 LIMIT 10
            """
        )

    def _is_column_included_for_profiling(self, column_name):
        # TODO use string.split() to separate table expr (with wildcard) from column expr (with wildcard) using  self.profile_columns_cfg
        return True

    def get_row_counts_for_all_tables(self) -> Dict[str, int]:
        """
        Returns a dict that maps table names to row counts.
        Later this could be implemented with different queries depending on the data source type.
        """
        include_tables = []

        if len(self.profile_columns_cfg.include_columns) == 0:
            include_tables.append("%")
        else:
            include_tables.extend(self._get_table_expression(self.profile_columns_cfg.include_columns))
        include_tables.extend(self._get_table_expression(self.profile_columns_cfg.exclude_columns))
        sql = self.data_source.sql_get_table_names_with_count(include_tables=include_tables)
        query = Query(
            data_source_scan=self.data_source_scan,
            unqualified_query_name="get_counts_by_tables_for_profile_columns",
            sql=sql,
        )
        query.execute()
        return {row[0]: row[1] for row in query.rows}

    def _get_table_expression(self, include_columns: List[str]) -> List[str]:
        table_expressions = []
        for include_column_expression in include_columns:
            parts = include_column_expression.split(".")
            if len(parts) != 2:
                self.logs.error(
                    f'Invalid include column expression "{include_column_expression}"',
                    location=self.profile_columns_cfg.location,
                )
            else:
                table_expression = parts[0]
                table_expressions.append(table_expression)
        return table_expressions
