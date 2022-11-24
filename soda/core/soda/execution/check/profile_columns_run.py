from __future__ import annotations

from numbers import Number
from typing import TYPE_CHECKING, overload

from soda.execution.query.query import Query
from soda.profiling.profile_columns_result import (
    ProfileColumnsResult,
    ProfileColumnsResultColumn,
    ProfileColumnsResultTable,
)
from soda.sodacl.data_source_check_cfg import ProfileColumnsCfg

if TYPE_CHECKING:
    from soda.execution.data_source_scan import DataSourceScan


@overload
def cast_dtype_handle_none(value: int | float | None, target_dtype: str = "float") -> float:
    ...


@overload
def cast_dtype_handle_none(value: int | float | None, target_dtype: str = "int") -> int:
    ...


def cast_dtype_handle_none(value: int | float | None, target_dtype: str | None = None) -> int | float | None:
    dtypes_map = {"int": int, "float": float}
    assert target_dtype is not None, "Target dtype cannot be None"
    assert (
        target_dtype in dtypes_map.keys()
    ), f"Unsupported target dtype: {target_dtype}. Can only be: {list(dtypes_map.keys())}"
    if value is not None:
        cast_function = dtypes_map[target_dtype]
        cast_value = cast_function(value)
        return cast_value


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
        include_tables = self._get_table_expression(self.profile_columns_cfg.include_columns)
        exclude_tables = self._get_table_expression(self.profile_columns_cfg.exclude_columns)

        # row_counts is a dict that maps table names to row counts.
        table_names: dict[str, int] = self.data_source.get_table_names(
            include_tables=include_tables,
            exclude_tables=exclude_tables,
            query_name="profile-columns-get-table-names",
        )

        if len(table_names) < 1:
            self.logs.warning(
                f"No table matching your SodaCL inclusion list found on your {self.data_source.data_source_name} "
                "data source. Profiling results may be incomplete or entirely skipped",
                location=self.profile_columns_cfg.location,
            )
            return profile_columns_result
        parsed_included_tables_and_columns = self._build_column_expression_list(
            self.profile_columns_cfg.include_columns
        )
        parsed_excluded_tables_and_columns = self._build_column_expression_list(
            self.profile_columns_cfg.exclude_columns
        )

        self.logs.info("Profiling columns for the following tables:")
        for table_name in table_names:
            self.logs.info(f"  - {table_name}")
            profile_columns_result_table = profile_columns_result.create_table(
                table_name, self.data_source.data_source_name, row_count=None
            )
            included_columns, excluded_columns = self.build_column_inclusion_exclusion_lists(
                table_name, parsed_included_tables_and_columns, parsed_excluded_tables_and_columns
            )
            # get columns & metadata for current table
            columns_metadata_result = self.data_source.get_table_columns(
                table_name=table_name,
                query_name=f"profile-columns-get-column-metadata-for-{table_name}",
                included_columns=included_columns,
                excluded_columns=excluded_columns,
            )
            if columns_metadata_result is not None:
                # perform numerical metrics collection
                numerical_columns = {
                    col_name: data_type
                    for col_name, data_type in columns_metadata_result.items()
                    if data_type in self.data_source.NUMERIC_TYPES_FOR_PROFILING
                }

                for column_name, column_type in numerical_columns.items():
                    try:
                        self.profile_numeric_column(
                            column_name,
                            column_type,
                            table_name,
                            profile_columns_result_table,
                        )
                    except Exception as e:
                        self.logs.error(
                            f"Problem profiling numeric column {table_name}.{column_name}: {e}",
                            exception=e,
                        )

                # text columns
                text_columns = {
                    col_name: data_type
                    for col_name, data_type in columns_metadata_result.items()
                    if data_type in self.data_source.TEXT_TYPES_FOR_PROFILING
                }
                for column_name, column_type in text_columns.items():
                    try:
                        self.profile_text_column(
                            column_name,
                            column_type,
                            table_name,
                            profile_columns_result_table,
                        )
                    except Exception as e:
                        self.logs.error(
                            f"Problem profiling text column {table_name}.{column_name}: {e}",
                            exception=e,
                        )

            else:
                self.logs.error(
                    f"No columns matching your SodaCL inclusion patterns were found on {table_name}.",
                    location=self.profile_columns_cfg.location,
                )
        return profile_columns_result

    def profile_numeric_column(
        self,
        column_name: str,
        column_type: str,
        table_name: str,
        profile_columns_result_table: ProfileColumnsResultTable,
    ):
        self.logs.debug(f"Profiling column {column_name} of {table_name}")
        profile_columns_result_column, is_included_column = self.build_profiling_column(
            column_name,
            column_type,
            profile_columns_result_table,
        )
        if profile_columns_result_column and is_included_column:
            value_frequencies_sql = self.data_source.profiling_sql_values_frequencies_query(
                "numeric",
                table_name,
                column_name,
                self.profile_columns_cfg.limit_mins_maxs,
                self.profile_columns_cfg.limit_frequent_values,
            )

            value_frequencies_query = Query(
                data_source_scan=self.data_source_scan,
                unqualified_query_name=f"profiling-{table_name}-{column_name}-value-frequencies-numeric",
                sql=value_frequencies_sql,
            )
            value_frequencies_query.execute()

            def unify_type(v):
                return float(v) if isinstance(v, Number) else v

            if value_frequencies_query.rows is not None:
                profile_columns_result_column.mins = [
                    unify_type(row[2]) for row in value_frequencies_query.rows if row[0] == "mins"
                ]
                profile_columns_result_column.maxs = [
                    unify_type(row[2]) for row in value_frequencies_query.rows if row[0] == "maxs"
                ]
                profile_columns_result_column.min = (
                    profile_columns_result_column.mins[0] if len(profile_columns_result_column.mins) >= 1 else None
                )
                profile_columns_result_column.max = (
                    profile_columns_result_column.maxs[0] if len(profile_columns_result_column.maxs) >= 1 else None
                )
                profile_columns_result_column.frequent_values = [
                    {"value": str(row[2]), "frequency": int(row[3])}
                    for row in value_frequencies_query.rows
                    if row[0] == "frequent_values"
                ]
            else:
                self.logs.error(
                    f"Database returned no results for minumum values, maximum values and frequent values in table: {table_name}, columns: {column_name}"
                )

            # pure aggregates
            aggregates_sql = self.data_source.profiling_sql_aggregates_numeric(table_name, column_name)
            aggregates_query = Query(
                data_source_scan=self.data_source_scan,
                unqualified_query_name=f"profiling-{table_name}-{column_name}-profiling-aggregates",
                sql=aggregates_sql,
            )
            aggregates_query.execute()
            if aggregates_query.rows is not None:
                profile_columns_result_column.average = cast_dtype_handle_none(aggregates_query.rows[0][0], "float")
                profile_columns_result_column.sum = cast_dtype_handle_none(aggregates_query.rows[0][1], "float")
                profile_columns_result_column.variance = cast_dtype_handle_none(aggregates_query.rows[0][2], "float")
                profile_columns_result_column.standard_deviation = cast_dtype_handle_none(
                    aggregates_query.rows[0][3], "float"
                )
                profile_columns_result_column.distinct_values = cast_dtype_handle_none(
                    aggregates_query.rows[0][4], "int"
                )
                profile_columns_result_column.missing_values = cast_dtype_handle_none(
                    aggregates_query.rows[0][5], "int"
                )
            else:
                self.logs.error(
                    f"Database returned no results for aggregates in table: {table_name}, columns: {column_name}"
                )

            # histogram
            if profile_columns_result_column.min is None:
                self.logs.warning("Min cannot be None, make sure the min metric is derived before histograms")
            if profile_columns_result_column.max is None:
                self.logs.warning("Max cannot be None, make sure the min metric is derived before histograms")

            if profile_columns_result_column.min is not None and profile_columns_result_column.max is not None:
                histogram_sql, bins_list = self.data_source.histogram_sql_and_boundaries(
                    table_name,
                    column_name,
                    profile_columns_result_column.min,
                    profile_columns_result_column.max,
                    profile_columns_result_column.distinct_values,
                    column_type,
                )
                if histogram_sql is not None:
                    histogram_query = Query(
                        data_source_scan=self.data_source_scan,
                        unqualified_query_name=f"profiling-{table_name}-{column_name}-histogram",
                        sql=histogram_sql,
                    )
                    histogram_query.execute()
                    histogram = {}
                    if histogram_query.rows is not None:
                        histogram["boundaries"] = bins_list
                        histogram["frequencies"] = [
                            int(freq) if freq is not None else 0 for freq in histogram_query.rows[0]
                        ]
                        profile_columns_result_column.histogram = histogram
                    else:
                        self.logs.error(
                            f"Database returned no results for histograms in table: {table_name}, columns: {column_name}"
                        )
            else:
                self.logs.warning(
                    f"Histogram query for {table_name}, column {column_name} skipped. See earlier warnings."
                )
        elif not is_included_column:
            self.logs.debug(f"Column: {column_name} in table: {table_name} is skipped from profiling by the user.")
        else:
            self.logs.error(
                f"No profiling information derived for column {column_name} in {table_name} and type: {column_type}. "
                "Soda Core could not create a column result."
            )

    def profile_text_column(
        self,
        column_name: str,
        column_type: str,
        table_name: str,
        profile_columns_result_table: ProfileColumnsResultTable,
    ):
        profile_columns_result_column, is_included_column = self.build_profiling_column(
            column_name,
            column_type,
            profile_columns_result_table,
        )
        if profile_columns_result_column and is_included_column:
            # frequent values for text column
            value_frequencies_sql = self.data_source.profiling_sql_values_frequencies_query(
                "text",
                table_name,
                column_name,
                self.profile_columns_cfg.limit_mins_maxs,
                self.profile_columns_cfg.limit_frequent_values,
            )
            value_frequencies_query = Query(
                data_source_scan=self.data_source_scan,
                unqualified_query_name=f"profiling-{table_name}-{column_name}-value-frequencies-text",
                sql=value_frequencies_sql,
            )
            value_frequencies_query.execute()
            if value_frequencies_query.rows:
                profile_columns_result_column.frequent_values = [
                    {"value": str(row[2]), "frequency": int(row[3])}
                    for row in value_frequencies_query.rows
                    if row[0] == "frequent_values"
                ]
            else:
                self.logs.warning(
                    f"Database returned no results for textual frequent values in {table_name}, column: {column_name}"
                )
            # pure text aggregates
            text_aggregates_sql = self.data_source.profiling_sql_aggregates_text(table_name, column_name)
            text_aggregates_query = Query(
                data_source_scan=self.data_source_scan,
                unqualified_query_name=f"profiling: {table_name}, {column_name}: get textual aggregates",
                sql=text_aggregates_sql,
            )
            text_aggregates_query.execute()
            if text_aggregates_query.rows:
                profile_columns_result_column.distinct_values = cast_dtype_handle_none(
                    text_aggregates_query.rows[0][0], "int"
                )
                profile_columns_result_column.missing_values = cast_dtype_handle_none(
                    text_aggregates_query.rows[0][1], "int"
                )
                profile_columns_result_column.average_length = cast_dtype_handle_none(
                    text_aggregates_query.rows[0][2], "int"
                )
                profile_columns_result_column.min_length = cast_dtype_handle_none(
                    text_aggregates_query.rows[0][3], "int"
                )
                profile_columns_result_column.max_length = cast_dtype_handle_none(
                    text_aggregates_query.rows[0][4], "int"
                )
            else:
                self.logs.error(
                    f"Database returned no results for textual aggregates in table: {table_name}, columns: {column_name}"
                )
        elif not is_included_column:
            self.logs.debug(f"Column: {column_name} in table: {table_name} is skipped from profiling by the user.")
        else:
            self.logs.error(
                f"No profiling information derived for column {column_name} in {table_name} and type: {column_type}. "
                "Soda Core could not create a column result."
            )

    def build_profiling_column(
        self,
        column_name: str,
        column_type: str,
        table_result: ProfileColumnsResultTable,
    ) -> tuple[ProfileColumnsResultColumn | None, bool]:
        profile_columns_result_column: ProfileColumnsResultColumn = table_result.create_column(column_name, column_type)
        return profile_columns_result_column, True

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
            or "%" in cols_for_all_tables
        ):
            if candidate_column_name in table_columns:
                return True
        else:
            return False

    def build_column_inclusion_exclusion_lists(
        self,
        table_name: str,
        included_columns: dict[str, list[str]],
        excluded_columns: dict[str, list[str]],
    ) -> tuple[list[str], list[str]]:
        _included_columns = included_columns.copy()
        _excluded_columns = excluded_columns.copy()
        included_all_tables = _included_columns.pop("%", [])
        excluded_all_tables = _excluded_columns.pop("%", [])
        table_name = table_name.lower()
        # get the table entry that matches the current table name (also think about potential regex matching)
        qualified_included_table_columns = _included_columns.get(table_name, [])
        # get the list of columns for that fully qualified entry out
        qualified_included_table_columns.extend(included_all_tables)
        # do the same for excludes
        qualified_excluded_table_columns = _excluded_columns.get(table_name, [])
        qualified_excluded_table_columns.extend(excluded_all_tables)

        return set(qualified_included_table_columns), set(qualified_excluded_table_columns)

    def _build_column_expression_list(self, columns_expression: list[str]) -> dict[str, list[str]]:
        selected_columns = {}
        for col_expression in columns_expression:
            table, column = col_expression.split(".")
            table = table.lower()
            if table in selected_columns.keys():
                if selected_columns[table] is None:
                    selected_columns[table] = [column]
                else:
                    selected_columns[table].append(column)
            else:
                selected_columns.update({table: [column]})
        return selected_columns

    def _get_table_expression(self, columns_expression: list[str], is_for_exclusion: bool = False) -> list[str]:
        table_expressions = []
        if is_for_exclusion:
            operation_word = "exclude"
        else:
            operation_word = "include"
        for column_expression in columns_expression:
            parts = column_expression.split(".")
            if len(parts) != 2:
                self.logs.error(
                    f'Invalid {operation_word} column expression "{column_expression}"',
                    location=self.profile_columns_cfg.location,
                )
            else:
                if is_for_exclusion:
                    table_expression = parts[0] if parts[1] == "%" else None
                else:
                    table_expression = parts[0]
                if table_expression is not None:
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
