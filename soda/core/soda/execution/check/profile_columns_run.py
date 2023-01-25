from __future__ import annotations

from typing import TYPE_CHECKING, overload

from soda.execution.query.query import Query
from soda.profiling.profile_columns_metadata import ProfilerTableMetadata
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

    @staticmethod
    def parse_profiling_expressions(profiling_expressions: list[str]) -> list[dict[str, str]]:
        parsed_profiling_expressions = []
        for profiling_expression in profiling_expressions:
            table_name_pattern, column_name_pattern = profiling_expression.split(".")
            parsed_profiling_expressions.append(
                {
                    "table_name_pattern": table_name_pattern,
                    "column_name_pattern": column_name_pattern,
                }
            )

        return parsed_profiling_expressions

    def run(self) -> ProfileColumnsResult:
        self.logs.info(f"Running column profiling for data source: {self.data_source.data_source_name}")

        profile_result_column_tables = ProfilerTableMetadata(
            data_source=self.data_source,
            include_columns=self.profile_columns_cfg.include_columns,
            exclude_columns=self.profile_columns_cfg.exclude_columns,
        ).get_profile_result_tables()

        profile_columns_result: ProfileColumnsResult = ProfileColumnsResult(self.profile_columns_cfg)

        if profile_result_column_tables is None:
            return self._raise_no_tables_to_profile_warning(profile_columns_result)

        self.logs.info("Profiling columns for the following tables:")
        for result_table in profile_result_column_tables:
            table_name = result_table.table_name
            self.logs.info(f"  - {table_name}")
            for result_column in result_table.result_columns:
                column_name = result_column.column_name
                column_data_type = result_column.column_type
                try:
                    if column_data_type in self.data_source.NUMERIC_TYPES_FOR_PROFILING:
                        profiling_column_type = "numeric"
                        self.profile_numeric_column(result_column, table_name)
                    elif column_data_type in self.data_source.TEXT_TYPES_FOR_PROFILING:
                        profiling_column_type = "text"
                        self.profile_text_column(
                            column_name,
                            column_data_type,
                            table_name,
                            result_table,
                        )
                    else:
                        self.logs.warning(
                            f"Column '{table_name}.{column_name}' was not profiled because column data "
                            f"type '{column_data_type}' is not in supported profiling data types"
                        )
                except Exception as e:
                    self.logs.error(
                        f"Problem profiling {profiling_column_type} column '{table_name}.{column_name}' with data type '{column_data_type}': {e}"
                    )

        return profile_columns_result

    def _compute_value_frequencies(self, table_name: str, column_name: str) -> list[tuple] | None:
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
        rows = value_frequencies_query.rows
        return rows

    def _compute_aggregated_metrics(self, table_name: str, column_name: str) -> list[tuple] | None:
        aggregates_sql = self.data_source.profiling_sql_aggregates_numeric(table_name, column_name)
        aggregates_query = Query(
            data_source_scan=self.data_source_scan,
            unqualified_query_name=f"profiling-{table_name}-{column_name}-profiling-aggregates",
            sql=aggregates_sql,
        )
        aggregates_query.execute()
        rows = aggregates_query.rows
        return rows

    def profile_numeric_column(
        self,
        result_column: ProfileColumnsResultColumn,
        table_name: str,
    ):
        column_name = result_column.column_name
        self.logs.debug(f"Profiling column {column_name} of {table_name}")

        # mins, maxs, frequent values
        value_frequencies = self._compute_value_frequencies(table_name, column_name)
        if value_frequencies is None:
            self.logs.error(
                "Database returned no results for minumum values, maximum values and "
                f"frequent values in table: {table_name}, columns: {column_name}"
            )
            return
        result_column.set_mins(value_frequencies=value_frequencies)
        result_column.set_maxs(value_frequencies=value_frequencies)
        result_column.set_min()
        result_column.set_max()
        result_column.set_frequent_values(value_frequencies=value_frequencies)

        # histogram
        if result_column.min is None:
            self.logs.warning("Min cannot be None, make sure the min metric is derived before histograms")
        if result_column.max is None:
            self.logs.warning("Max cannot be None, make sure the min metric is derived before histograms")
        if result_column.min is None or result_column.max is None:

            return
        aggregated_metrics = self._compute_aggregated_metrics(table_name, column_name)
        if aggregated_metrics is None:
            self.logs.error(
                "Database returned no results for aggregates in table: {table_name}, columns: {column_name}"
            )
            return

        result_column.average = cast_dtype_handle_none(aggregates_query.rows[0][0], "float")
        result_column.sum = cast_dtype_handle_none(aggregates_query.rows[0][1], "float")
        result_column.variance = cast_dtype_handle_none(aggregates_query.rows[0][2], "float")
        result_column.standard_deviation = cast_dtype_handle_none(aggregates_query.rows[0][3], "float")
        result_column.distinct_values = cast_dtype_handle_none(aggregates_query.rows[0][4], "int")
        result_column.missing_values = cast_dtype_handle_none(aggregates_query.rows[0][5], "int")

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
            self.logs.warning(f"Histogram query for {table_name}, column {column_name} skipped. See earlier warnings.")
        # elif not is_included_column:
        #     self.logs.debug(f"Column: {column_name} in table: {table_name} is skipped from profiling by the user.")
        # else:
        #     self.logs.error(
        #         f"No profiling information derived for column {column_name} in {table_name} and type: {column_type}. "
        #         "Soda Core could not create a column result."
        #     )

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

    def _raise_no_tables_to_profile_warning(self, profile_columns_result: ProfileColumnsResult) -> ProfileColumnsResult:
        self.logs.warning(
            "Your SodaCL profiling expressions did not return any existing dataset name"
            f" and column name combinations for your '{self.data_source.data_source_name}' "
            "data source. Please make sure that the patterns in your profiling expressions define "
            "existing dataset name and column name combinations."
            " Profiling results may be incomplete or entirely skipped. See the docs for more information: \n"
            f"https://go.soda.io/display-profile",
            location=self.profile_columns_cfg.location,
        )
        return profile_columns_result
