from __future__ import annotations

from typing import TYPE_CHECKING

from soda.execution.query import Query
from soda.profiling.profile_columns_result import ProfileColumnsResult
from soda.profiling.profile_columns_result_column import ProfileColumnsResultColumn
from soda.profiling.profile_columns_result_table import ProfileColumnsResultTable
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
        self.logs.info(f"Running column profiling for data source: {self.data_source.data_source_name}")

        # row_counts is a dict that maps table names to row counts.
        row_counts_by_table_name: dict[str, int] = self.data_source.get_row_counts_all_tables(
            include_tables=self._get_table_expression(self.profile_columns_cfg.include_columns),
            exclude_tables=self._get_table_expression(self.profile_columns_cfg.exclude_columns),
            query_name="profile columns: get tables and row counts",
        )
        parsed_tables_and_columns = self._build_column_inclusion(self.profile_columns_cfg.include_columns)
        for table_name in row_counts_by_table_name:
            self.logs.debug(f"Profiling columns for {table_name}")
            measured_row_count = row_counts_by_table_name[table_name]
            profile_columns_result_table = profile_columns_result.create_table(
                table_name, self.data_source.data_source_name, measured_row_count
            )

            # get columns & metadata for current table
            columns_metadata_sql = self.data_source.sql_to_get_column_metadata_for_table(table_name)
            columns_metadata_query = Query(
                data_source_scan=self.data_source_scan,
                unqualified_query_name=f"get col metadata for table: {table_name}",
                sql=columns_metadata_sql,
            )
            columns_metadata_query.execute()
            assert columns_metadata_query.rows, f"No metadata was captured for table: {table_name}"
            columns_metadata_result = {column[0]: column[1] for column in columns_metadata_query.rows}

            # perform numerical metrics collection
            numerical_columns = {
                col_name: data_type
                for col_name, data_type in columns_metadata_result.items()
                if data_type in self.data_source.NUMERIC_TYPES_FOR_PROFILING
            }

            for column_name, column_type in numerical_columns.items():
                self.profile_numeric_column(
                    column_name,
                    column_type,
                    table_name,
                    columns_metadata_result,
                    parsed_tables_and_columns,
                    profile_columns_result_table,
                )

            # text columns
            text_columns = {
                col_name: data_type
                for col_name, data_type in columns_metadata_result.items()
                if data_type in self.data_source.TEXT_TYPES_FOR_PROFILING
            }
            for column_name, column_type in text_columns.items():
                self.profile_text_column(
                    column_name,
                    column_type,
                    table_name,
                    columns_metadata_result,
                    parsed_tables_and_columns,
                    profile_columns_result_table,
                )

        if not profile_columns_result.tables:
            self.logs.error(f"Profiling for data source: {self.data_source.data_source_name} failed")
        return profile_columns_result

    def profile_numeric_column(
        self,
        column_name: str,
        column_type: str,
        table_name: str,
        columns_metadata_result: dict,
        parsed_tables_and_columns: dict[str, list[str]],
        profile_columns_result_table: ProfileColumnsResultTable,
    ):
        self.logs.debug(f"Profiling column {column_name} of {table_name}")
        profile_columns_result_column, is_included_column = self.build_profiling_column(
            column_name,
            column_type,
            table_name,
            list(columns_metadata_result.keys()),
            parsed_tables_and_columns,
            profile_columns_result_table,
        )
        if profile_columns_result_column and is_included_column:
            value_frequencies_sql = self.data_source.profiling_sql_values_frequencies_query(table_name, column_name)

            value_frequencies_query = Query(
                data_source_scan=self.data_source_scan,
                unqualified_query_name=f"profiling: {table_name}, {column_name}: mins, maxes and values frequencies",
                sql=value_frequencies_sql,
            )
            value_frequencies_query.execute()
            if value_frequencies_query.rows is not None:
                profile_columns_result_column.mins = [
                    float(row[0]) if not isinstance(row[0], int) else row[0] for row in value_frequencies_query.rows
                ]
                profile_columns_result_column.maxes = [
                    float(row[1]) if not isinstance(row[1], int) else row[1] for row in value_frequencies_query.rows
                ]
                profile_columns_result_column.min = profile_columns_result_column.mins[0]
                profile_columns_result_column.max = profile_columns_result_column.maxes[0]
                profile_columns_result_column.frequent_values = self.build_frequent_values_dict(
                    values=[row[2] for row in value_frequencies_query.rows],
                    frequencies=[row[3] for row in value_frequencies_query.rows],
                )
            else:
                self.logs.error(
                    f"Database returned no results for minumum values, maximum values and frequent values in table: {table_name}, columns: {column_name}"
                )

            # pure aggregates
            aggregates_sql = self.data_source.profiling_sql_numeric_aggregates(table_name, column_name)
            aggregates_query = Query(
                data_source_scan=self.data_source_scan,
                unqualified_query_name=f"profiling: {table_name}, {column_name}: get_pure_profiling_aggregates",
                sql=aggregates_sql,
            )
            aggregates_query.execute()
            if aggregates_query.rows is not None:
                profile_columns_result_column.average = float(aggregates_query.rows[0][0])
                profile_columns_result_column.sum = float(aggregates_query.rows[0][1])
                profile_columns_result_column.variance = float(aggregates_query.rows[0][2])
                profile_columns_result_column.standard_deviation = float(aggregates_query.rows[0][3])
                profile_columns_result_column.distinct_values = int(aggregates_query.rows[0][4])
                profile_columns_result_column.missing_values = int(aggregates_query.rows[0][5])
            else:
                self.logs.error(
                    f"Database returned no results for aggregates in table: {table_name}, columns: {column_name}"
                )

            # histogram
            assert (
                profile_columns_result_column.min is not None
            ), "Min cannot be None, make sure the min metric is derived before histograms"
            assert (
                profile_columns_result_column.max is not None
            ), "Max cannot be None, make sure the min metric is derived before histograms"
            histogram_sql, bins_list = self.data_source.histogram_sql_and_boundaries(
                table_name, column_name, profile_columns_result_column.min, profile_columns_result_column.max
            )
            histogram_query = Query(
                data_source_scan=self.data_source_scan,
                unqualified_query_name=f"profiling: {table_name}, {column_name}: get histogram",
                sql=histogram_sql,
            )
            histogram_query.execute()
            histogram = {}
            if histogram_query.rows is not None:
                histogram["boundaries"] = bins_list
                histogram["frequencies"] = [int(freq) if freq is not None else 0 for freq in histogram_query.rows[0]]
                profile_columns_result_column.histogram = histogram
            else:
                self.logs.error(
                    f"Database returned no results for histograms in table: {table_name}, columns: {column_name}"
                )
        else:
            self.logs.info(f"No profiling information derived for column {column_name} in {table_name}")

    def profile_text_column(
        self,
        column_name: str,
        column_type: str,
        table_name: str,
        columns_metadata_result: dict,
        parsed_tables_and_columns: dict[str, list[str]],
        profile_columns_result_table: ProfileColumnsResultTable,
    ):
        profile_columns_result_column, is_included_column = self.build_profiling_column(
            column_name,
            column_type,
            table_name,
            list(columns_metadata_result.keys()),
            parsed_tables_and_columns,
            profile_columns_result_table,
        )
        if profile_columns_result_column and is_included_column:
            # frequent values for text column
            frequent_values_sql = self.data_source.profiling_sql_top_values(table_name, column_name)
            frequent_values_query = Query(
                data_source_scan=self.data_source_scan,
                unqualified_query_name=f"profiling: {table_name}, {column_name}: get frequent values text cols",
                sql=frequent_values_sql,
            )
            frequent_values_query.execute()
            if frequent_values_query.rows:
                profile_columns_result_column.frequent_values = self.build_frequent_values_dict(
                    values=[row[2] for row in frequent_values_query.rows],
                    frequencies=[row[0] for row in frequent_values_query.rows],
                )
            else:
                self.logs.error(
                    f"Database returned no results for textual frequent values in {table_name}, column: {column_name}"
                )
            # pure text aggregates
            text_aggregates_sql = self.data_source.profiling_sql_text_aggregates(table_name, column_name)
            text_aggregates_query = Query(
                data_source_scan=self.data_source_scan,
                unqualified_query_name=f"profiling: {table_name}, {column_name}: get textual aggregates",
                sql=text_aggregates_sql,
            )
            text_aggregates_query.execute()
            if text_aggregates_query.rows:
                profile_columns_result_column.distinct_values = int(text_aggregates_query.rows[0][0])
                profile_columns_result_column.missing_values = int(text_aggregates_query.rows[0][1])
                profile_columns_result_column.average_length = (
                    int(text_aggregates_query.rows[0][2]) if text_aggregates_query.rows[0][2] is not None else None
                )
                profile_columns_result_column.min_length = (
                    int(text_aggregates_query.rows[0][3]) if text_aggregates_query.rows[0][3] is not None else None
                )
                profile_columns_result_column.max_length = (
                    int(text_aggregates_query.rows[0][4]) if text_aggregates_query.rows[0][4] is not None else None
                )
            else:
                self.logs.error(
                    f"Database returned no results for textual aggregates in table: {table_name}, columns: {column_name}"
                )
        else:
            self.logs.info(f"No profiling information derived for column {column_name} in {table_name}")

    def build_profiling_column(
        self,
        column_name: str,
        column_type: str,
        table_name: str,
        table_columns: list[str],
        parsed_tables_and_columns: dict[str, list[str]],
        table_result: ProfileColumnsResultTable,
    ) -> tuple[ProfileColumnsResultColumn | None, bool]:
        column_name = column_name.lower()
        if self._is_column_included_for_profiling(column_name, table_name, table_columns, parsed_tables_and_columns):
            profile_columns_result_column: ProfileColumnsResultColumn = table_result.create_column(
                column_name, column_type
            )
            return profile_columns_result_column, True
        return None, False

    @staticmethod
    def build_frequent_values_dict(values: list[str | int | float], frequencies: list[int]) -> list[dict[str, int]]:
        frequent_values = []
        for i, value in enumerate(values):
            frequent_values.append({"value": str(value), "frequency": frequencies[i]})
        return frequent_values

    def _is_column_included_for_profiling(
        self,
        candidate_column_name: str,
        table_name: str,
        table_columns: list[str],
        parsed_tables_and_columns: dict[str, list[str]],
    ):

        table_name = table_name.lower()
        candidate_column_name = candidate_column_name.lower()
        cols_for_all_tables = parsed_tables_and_columns.get("%", [])
        table_columns = [col_name.lower() for col_name in table_columns]
        if (
            candidate_column_name in parsed_tables_and_columns.get(table_name, [])
            or candidate_column_name in cols_for_all_tables
            or parsed_tables_and_columns.get(table_name, []) == ["%"]
        ):
            if candidate_column_name in table_columns:
                return True
        else:
            return False

    # TODO: Deal with exclude set as well
    def _build_column_inclusion(self, columns_expression: list[str]) -> dict[str, list[str]]:
        included_columns = {}
        for col_expression in columns_expression:
            table, column = col_expression.split(".")
            table = table.lower()
            if table in included_columns.keys():
                if included_columns[table] is None:
                    included_columns[table] = [column]
                else:
                    included_columns[table].append(column)
            else:
                included_columns.update({table: [column]})
        return included_columns

    def _get_table_expression(self, columns_expression: list[str]) -> list[str]:
        table_expressions = []
        for column_expression in columns_expression:
            parts = column_expression.split(".")
            if len(parts) != 2:
                self.logs.error(
                    f'Invalid include column expression "{column_expression}"',
                    location=self.profile_columns_cfg.location,
                )
            else:
                table_expression = parts[0]
                table_expressions.append(table_expression)
        return table_expressions

    def _get_colums(self, columns_expression: list[str]) -> list[str]:
        column_expressions = []
        for column_expression in columns_expression:
            parts = column_expression.split(".")
            if len(parts) != 2:
                self.logs.error(
                    f"Invalid include columns expression '{column_expression}'",
                    location=self.profile_columns_cfg.location,
                )
            else:
                column_expression = parts[1]
                column_expressions.append(column_expression)
        return column_expressions
