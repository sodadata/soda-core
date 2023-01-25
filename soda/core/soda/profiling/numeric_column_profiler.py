from __future__ import annotations

from typing import TYPE_CHECKING

from soda.execution.query.query import Query

if TYPE_CHECKING:
    from soda.execution.data_source_scan import DataSourceScan
    from soda.profiling.profile_columns_result import ProfileColumnsResultColumn
    from soda.sodacl.data_source_check_cfg import ProfileColumnsCfg


class NumericColumnProfiler:
    def __init__(
        self,
        data_source_scan: DataSourceScan,
        profile_columns_cfg: ProfileColumnsCfg,
        table_name: str,
        result_column: ProfileColumnsResultColumn,
    ) -> None:
        self.data_source_scan = data_source_scan
        self.data_source = data_source_scan.data_source
        self.logs = data_source_scan.scan._logs
        self.profile_columns_cfg = profile_columns_cfg
        self.table_name = table_name
        self.column_name = result_column.column_name
        self.result_column = result_column

    def profile(self) -> ProfileColumnsResultColumn:
        self.logs.debug(f"Profiling column {self.column_name} of {self.table_name}")

        # mins, maxs, min, max, frequent values
        value_frequencies = self._compute_value_frequency()
        if value_frequencies is None:
            self.logs.error(
                "Database returned no results for minumum values, maximum values and "
                f"frequent values in table: {self.table_name}, columns: {self.column_name}"
            )
            return self.result_column
        self.result_column.set_frequency_metrics(value_frequencies=value_frequencies)

        # Average, sum, variance, standard deviation, distinct values, missing values
        aggregated_metrics = self._compute_aggregated_metrics()
        if aggregated_metrics is None:
            self.logs.error(
                "Database returned no results for aggregates in table: {table_name}, columns: {column_name}"
            )
            return self.result_column
        self.result_column.set_aggregation_metrics(aggregated_metrics=aggregated_metrics)

        # histogram
        histogram_values = self._compute_histogram()
        if histogram_values is None:
            self.logs.error(
                f"Database returned no results for histograms in table: {self.table_name}, columns: {self.column_name}"
            )
            return self.result_column
        self.result_column.set_histogram(histogram_values=histogram_values)
        return self.result_column

    def _compute_value_frequency(self) -> list[tuple] | None:
        value_frequencies_sql = self.data_source.profiling_sql_values_frequencies_query(
            "numeric",
            self.table_name,
            self.column_name,
            self.profile_columns_cfg.limit_mins_maxs,
            self.profile_columns_cfg.limit_frequent_values,
        )

        value_frequencies_query = Query(
            data_source_scan=self.data_source_scan,
            unqualified_query_name=f"profiling-{self.table_name}-{self.column_name}-value-frequencies-numeric",
            sql=value_frequencies_sql,
        )
        value_frequencies_query.execute()
        rows = value_frequencies_query.rows
        return rows

    def _compute_aggregated_metrics(self) -> list[tuple] | None:
        aggregates_sql = self.data_source.profiling_sql_aggregates_numeric(self.table_name, self.column_name)
        aggregates_query = Query(
            data_source_scan=self.data_source_scan,
            unqualified_query_name=f"profiling-{self.table_name}-{self.column_name}-profiling-aggregates",
            sql=aggregates_sql,
        )
        aggregates_query.execute()
        rows = aggregates_query.rows
        return rows

    def _compute_histogram(self) -> None | dict[str, list]:
        if self.result_column.min is None:
            self.logs.warning("Min cannot be None, make sure the min metric is derived before histograms")
        if self.result_column.max is None:
            self.logs.warning("Max cannot be None, make sure the min metric is derived before histograms")
        if self.result_column.distinct_values is None:
            self.logs.warning(
                "Distinct values cannot be None, make sure the distinct values metric is derived before histograms"
            )
        if (
            self.result_column.min is None
            or self.result_column.max is None
            or self.result_column.distinct_values is None
        ):
            self.logs.warning(
                f"Histogram query for {self.table_name}, column {self.column_name} skipped. See earlier warnings."
            )
            return None

        histogram_sql, bins_list = self.data_source.histogram_sql_and_boundaries(
            table_name=self.table_name,
            column_name=self.column_name,
            min_value=self.result_column.min,
            max_value=self.result_column.max,
            n_distinct=self.result_column.distinct_values,
            column_type=self.result_column.column_data_type,
        )
        if histogram_sql is None:
            return None

        histogram_query = Query(
            data_source_scan=self.data_source_scan,
            unqualified_query_name=f"profiling-{self.table_name}-{self.column_name}-histogram",
            sql=histogram_sql,
        )
        histogram_query.execute()
        histogram_values = histogram_query.rows

        if histogram_values is None:
            return None
        histogram = {}
        histogram["boundaries"] = bins_list
        histogram["frequencies"] = [int(freq) if freq is not None else 0 for freq in histogram_values[0]]
        return histogram
