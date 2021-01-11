#  Copyright 2020 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
from math import floor, ceil
from typing import List, AnyStr, Optional

from jinja2 import Template

from sodasql.scan.column_metadata import ColumnMetadata
from sodasql.scan.measurement import Measurement
from sodasql.scan.metric import Metric
from sodasql.scan.scan_column import ScanColumn
from sodasql.scan.scan_yml import ScanYml
from sodasql.scan.scan_result import ScanResult
from sodasql.scan.sql_metric_yml import SqlMetricYml
from sodasql.scan.warehouse import Warehouse
from sodasql.soda_client.soda_client import SodaClient


class Scan:

    def __init__(self,
                 warehouse: Warehouse,
                 scan_yml: ScanYml = None,
                 sql_metrics: List[SqlMetricYml] = None,
                 soda_client: SodaClient = None,
                 variables: dict = None,
                 timeslice: str = None):
        self.soda_client: SodaClient = soda_client
        self.warehouse: Warehouse = warehouse
        self.dialect = warehouse.dialect

        self.scan_yml: ScanYml = scan_yml
        self.sql_metrics: List[SqlMetricYml] = sql_metrics

        # caches measurements that are not yet sent to soda and not yet added to self.scan_result
        # see also self.flush_measurements()
        self.measurements = []
        self.scan_result = ScanResult(timeslice)

        self.qualified_table_name = self.dialect.qualify_table_name(scan_yml.table_name)
        self.table_sample_clause = \
            f'\nTABLESAMPLE {scan_yml.sample_method}({scan_yml.sample_percentage})' \
            if scan_yml.sample_percentage \
            else ''

        self.variables = variables
        self.time_filter_sql = None
        if scan_yml.time_filter_template:
            if not variables:
                raise RuntimeError(f'No variables provided while time_filter "{str(scan_yml.time_filter)}" specified')
            self.time_filter_sql = scan_yml.time_filter_template.render(variables)

        self.scan_reference = {
            'warehouse': warehouse.name,
            'table': scan_yml.table_name,
            'time': ...
        }

        self.columns: List[ColumnMetadata] = []
        self.column_names: List[str] = []
        # maps column names (lower case) to ScanColumn's
        self.scan_columns: dict = {}

    def execute(self) -> ScanResult:
        if self.scan_yml:
            self.query_columns_metadata()
            self.flush_measurements()

            self.query_aggregations()
            self.flush_measurements()

            self.query_group_by_value()
            self.flush_measurements()

            self.query_histograms()
            self.flush_measurements()

        if self.sql_metrics:
            self.query_sql_metrics_and_run_tests()
            self.flush_measurements()

        self.run_table_tests()
        self.run_column_tests()

        return self.scan_result

    def query_columns_metadata(self):
        sql = self.warehouse.dialect.sql_columns_metadata_query(self.scan_yml)
        column_tuples = self.warehouse.sql_fetchall(sql)
        self.columns = []
        for column_tuple in column_tuples:
            name = column_tuple[0]
            type = column_tuple[1]
            nullable = 'YES' == column_tuple[2].upper()
            self.columns.append(ColumnMetadata(name, type, nullable))
        logging.debug(str(len(self.columns))+' columns:')
        for column in self.columns:
            logging.debug(f'  {column.name} {column.type} {"" if column.nullable else "not null"}')

        self.column_names: List[str] = [column_metadata.name for column_metadata in self.columns]
        self.scan_columns: dict = {column.name.lower(): ScanColumn(self, column) for column in self.columns}

        self.add_query(Measurement(Metric.SCHEMA, value=self.columns))

    def query_aggregations(self):
        # This measurements list is used to match measurements with the query field order.
        # After query execution, the value of the measurements will be extracted from the query result and
        # the measurements will be added with self.add_query(measurement)
        measurements: List[Measurement] = []

        fields: List[str] = []

        dialect = self.warehouse.dialect

        if self.scan_yml.is_metric_enabled(Metric.ROW_COUNT):
            fields.append(dialect.sql_expr_count_all())
            measurements.append(Measurement(Metric.ROW_COUNT))

        # maps db column names (lower) to missing and invalid metric indices in the measurements
        # eg { 'colname': {'missing': 2, 'invalid': 3}, ...}
        column_metric_indices = {}

        for column_name_lower in self.scan_columns:
            metric_indices = {}
            column_metric_indices[column_name_lower] = metric_indices
            scan_column: ScanColumn = self.scan_columns[column_name_lower]
            column_name = scan_column.column_name

            if scan_column.is_missing_enabled:
                metric_indices['non_missing'] = len(measurements)
                if scan_column.non_missing_condition:
                    fields.append(dialect.sql_expr_count_conditional(scan_column.non_missing_condition))
                else:
                    fields.append(dialect.sql_expr_count_column(scan_column.qualified_column_name))
                measurements.append(Measurement(Metric.VALUES_COUNT, column_name))

            if scan_column.is_valid_enabled:
                metric_indices['valid'] = len(measurements)
                if scan_column.non_missing_and_valid_condition:
                    fields.append(dialect.sql_expr_count_conditional(scan_column.non_missing_and_valid_condition))
                else:
                    fields.append(dialect.sql_expr_count_column(scan_column.qualified_column_name))
                measurements.append(Measurement(Metric.VALID_COUNT, column_name))

            if scan_column.is_text:
                length_expr = dialect.sql_expr_conditional(
                        scan_column.non_missing_and_valid_condition,
                        dialect.sql_expr_length(scan_column.qualified_column_name)) \
                    if scan_column.non_missing_and_valid_condition \
                    else dialect.sql_expr_length(scan_column.qualified_column_name)

                if self.scan_yml.is_metric_enabled(Metric.MIN_LENGTH, column_name):
                    fields.append(dialect.sql_expr_min(length_expr))
                    measurements.append(Measurement(Metric.MIN_LENGTH, column_name))

                if self.scan_yml.is_metric_enabled(Metric.MAX_LENGTH, column_name):
                    fields.append(dialect.sql_expr_max(length_expr))
                    measurements.append(Measurement(Metric.MAX_LENGTH, column_name))

            if scan_column.has_numeric_values:
                if scan_column.is_metric_enabled(Metric.MIN):
                    fields.append(dialect.sql_expr_min(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.MIN, column_name))

                if scan_column.is_metric_enabled(Metric.MAX):
                    fields.append(dialect.sql_expr_max(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.MAX, column_name))

                if scan_column.is_metric_enabled(Metric.AVG):
                    fields.append(dialect.sql_expr_avg(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.AVG, column_name))

                if scan_column.is_metric_enabled(Metric.SUM):
                    fields.append(dialect.sql_expr_sum(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.SUM, column_name))

                if scan_column.is_metric_enabled(Metric.VARIANCE):
                    fields.append(dialect.sql_expr_variance(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.VARIANCE, column_name))

                if scan_column.is_metric_enabled(Metric.STDDEV):
                    fields.append(dialect.sql_expr_stddev(scan_column.numeric_expr))
                    measurements.append(Measurement(Metric.STDDEV, column_name))

        if len(fields) > 0:
            sql = 'SELECT \n  ' + ',\n  '.join(fields) + ' \n' \
                  'FROM ' + self.qualified_table_name
            if self.table_sample_clause:
                sql += f'\n{self.table_sample_clause}'
            if self.time_filter_sql:
                sql += f'\nWHERE {self.time_filter_sql}'

            query_result_tuple = self.warehouse.sql_fetchone(sql)

            for i in range(0, len(measurements)):
                measurement = measurements[i]
                measurement.value = query_result_tuple[i]
                self.add_query(measurement)

            # Calculating derived measurements
            row_count_measurement = next((m for m in measurements if m.metric == Metric.ROW_COUNT), None)
            if row_count_measurement:
                row_count = row_count_measurement.value
                for column_name_lower in self.scan_columns:
                    scan_column = self.scan_columns[column_name_lower]
                    column_name = scan_column.column_name
                    metric_indices = column_metric_indices[column_name_lower]
                    non_missing_index = metric_indices.get('non_missing')
                    if non_missing_index is not None:
                        values_count = measurements[non_missing_index].value
                        missing_count = row_count - values_count
                        missing_percentage = missing_count * 100 / row_count
                        values_percentage = values_count * 100 / row_count
                        self.add_derived(Measurement(Metric.MISSING_PERCENTAGE, column_name, missing_percentage))
                        self.add_derived(Measurement(Metric.MISSING_COUNT, column_name, missing_count))
                        self.add_derived(Measurement(Metric.VALUES_PERCENTAGE, column_name, values_percentage))

                        valid_index = metric_indices.get('valid')
                        if valid_index is not None:
                            valid_count = measurements[valid_index].value
                            invalid_count = row_count - missing_count - valid_count
                            invalid_percentage = invalid_count * 100 / row_count
                            valid_percentage = valid_count * 100 / row_count
                            self.add_derived(Measurement(Metric.INVALID_PERCENTAGE, column_name, invalid_percentage))
                            self.add_derived(Measurement(Metric.INVALID_COUNT, column_name, invalid_count))
                            self.add_derived(Measurement(Metric.VALID_PERCENTAGE, column_name, valid_percentage))

    def query_group_by_value(self):
        for column_name_lower in self.scan_columns:
            scan_column: ScanColumn = self.scan_columns[column_name_lower]
            column_name = scan_column.column_name

            if scan_column.is_any_metric_enabled(
                    [Metric.DISTINCT, Metric.UNIQUENESS, Metric.UNIQUE_COUNT,
                     Metric.MINS, Metric.MAXS, Metric.FREQUENT_VALUES, Metric.DUPLICATE_COUNT]):

                group_by_cte = scan_column.get_group_by_cte()
                numeric_value_expr = scan_column.get_group_by_cte_numeric_value_expression()

                if self.scan_yml.is_any_metric_enabled(
                        [Metric.DISTINCT, Metric.UNIQUENESS, Metric.UNIQUE_COUNT, Metric.DUPLICATE_COUNT],
                        column_name):

                    sql = (f'{group_by_cte} \n'
                           f'SELECT COUNT(*), \n'
                           f'       COUNT(CASE WHEN frequency = 1 THEN 1 END), \n'
                           f'       SUM(frequency) \n'
                           f'FROM group_by_value')

                    query_result_tuple = self.warehouse.sql_fetchone(sql)
                    distinct_count = query_result_tuple[0]
                    unique_count = query_result_tuple[1]
                    valid_count = query_result_tuple[2] if query_result_tuple[2] else 0
                    duplicate_count = distinct_count - unique_count
                    uniqueness = (distinct_count - 1) * 100 / (valid_count - 1)

                    self.add_query(Measurement(Metric.DISTINCT, column_name, distinct_count))
                    self.add_query(Measurement(Metric.UNIQUE_COUNT, column_name, unique_count))
                    self.add_derived(Measurement(Metric.DUPLICATE_COUNT, column_name, duplicate_count))
                    self.add_derived(Measurement(Metric.UNIQUENESS, column_name, uniqueness))

                if scan_column.is_metric_enabled(Metric.MINS) and scan_column.numeric_expr is not None:

                    sql = (f'{group_by_cte} \n'
                           f'SELECT value \n'
                           f'FROM group_by_value \n'
                           f'ORDER BY {numeric_value_expr} ASC \n'
                           f'LIMIT {scan_column.mins_maxs_limit} \n')

                    rows = self.warehouse.sql_fetchall(sql)
                    mins = [row[0] for row in rows]
                    self.add_query(Measurement(Metric.MINS, column_name, mins))

                if self.scan_yml.is_metric_enabled(Metric.MAXS, column_name) \
                        and (scan_column.is_number or scan_column.is_column_numeric_text_format):

                    sql = (f'{group_by_cte} \n'
                           f'SELECT value \n'
                           f'FROM group_by_value \n'
                           f'ORDER BY {numeric_value_expr} DESC \n'
                           f'LIMIT {scan_column.mins_maxs_limit} \n')

                    rows = self.warehouse.sql_fetchall(sql)
                    maxs = [row[0] for row in rows]
                    self.add_query(Measurement(Metric.MAXS, column_name, maxs))

                if self.scan_yml.is_metric_enabled(Metric.FREQUENT_VALUES, column_name) \
                        and (scan_column.is_number or scan_column.is_column_numeric_text_format):

                    frequent_values_limit = self.scan_yml.get_frequent_values_limit(column_name)
                    sql = (f'{group_by_cte} \n'
                           f'SELECT value \n'
                           f'FROM group_by_value \n'
                           f'ORDER BY frequency DESC \n'
                           f'LIMIT {frequent_values_limit} \n')

                    rows = self.warehouse.sql_fetchall(sql)
                    frequent_values = [row[0] for row in rows]
                    self.add_query(Measurement(Metric.FREQUENT_VALUES, column_name, frequent_values))

    def query_histograms(self):
        for column_name_lower in self.scan_columns:
            scan_column: ScanColumn = self.scan_columns[column_name_lower]
            column_name = scan_column.column_name

            if scan_column.is_metric_enabled(Metric.HISTOGRAM):

                buckets: int = scan_column.get_histogram_buckets()

                min_value = scan_column.get_metric_value(Metric.MIN)
                max_value = scan_column.get_metric_value(Metric.MAX)

                if scan_column.has_numeric_values and min_value and max_value and min_value < max_value:
                    # Build the histogram query
                    min_value = floor(min_value * 1000) / 1000
                    max_value = ceil(max_value * 1000) / 1000
                    bucket_width = (max_value - min_value) / buckets

                    boundary = min_value
                    boundaries = [min_value]
                    for i in range(0, buckets):
                        boundary += bucket_width
                        boundaries.append(round(boundary, 3))

                    group_by_cte = scan_column.get_group_by_cte()
                    numeric_value_expr = scan_column.get_group_by_cte_numeric_value_expression()

                    field_clauses = []
                    for i in range(0, buckets):
                        lower_bound = '' if i == 0 else f'{boundaries[i]} <= {numeric_value_expr}'
                        upper_bound = '' if i == buckets - 1 else f'{numeric_value_expr} < {boundaries[i + 1]}'
                        optional_and = '' if lower_bound == '' or upper_bound == '' else ' and '
                        field_clauses.append(f'SUM(CASE WHEN {lower_bound}{optional_and}{upper_bound} THEN frequency END)')

                    fields = ',\n  '.join(field_clauses)

                    sql = (f'{group_by_cte} \n'
                           f'SELECT \n'
                           f'  {fields} \n'
                           f'FROM group_by_value')

                    if self.time_filter_sql:
                        sql += f' \nWHERE {self.scan.time_filter_sql}'

                    row = self.warehouse.sql_fetchone(sql)

                    # Process the histogram query
                    frequencies = []
                    for i in range(0, buckets):
                        frequency = row[i]
                        frequencies.append(0 if not frequency else int(frequency))
                    histogram = {
                        'boundaries': boundaries,
                        'frequencies': frequencies
                    }

                    self.add_query(Measurement(Metric.HISTOGRAM, column_name, histogram))

    def query_sql_metrics_and_run_tests(self):
        if self.sql_metrics:
            for sql_metric in self.sql_metrics:

                if self.variables:
                    sql_variables = self.variables.copy() if self.variables else {}
                    # TODO add functions to sql_variables that can convert scan variables to valid SQL literals
                    template = Template(sql_metric.sql)
                    resolved_sql = template.render(self.variables)
                else:
                    resolved_sql = sql_metric.sql

                if sql_metric.group_fields:
                    self.run_sql_metric_with_groups_and_run_tests(sql_metric, resolved_sql)
                else:
                    self.run_sql_metric_default_and_run_tests(sql_metric, resolved_sql)

    def run_sql_metric_with_groups_and_run_tests(self, sql_metric: SqlMetricYml, resolved_sql: AnyStr):
        group_fields_lower = set(group_field.lower() for group_field in sql_metric.group_fields)
        row_tuples, description = self.warehouse.sql_fetchall_description(resolved_sql)
        for row_tuple in row_tuples:
            group_measurements = []
            group_values = {}
            field_values = {}
            for i in range(len(row_tuple)):
                field_name = description[i][0]
                field_value = row_tuple[i]
                if field_name.lower() in group_fields_lower:
                    group_values[field_name.lower()] = field_value
                else:
                    group_measurements.append(Measurement(metric=field_name,
                                                          value=field_value,
                                                          group_values=group_values))
                    field_values[field_name] = field_value

            for group_measurement in group_measurements:
                logging.debug(f'SQL metric {sql_metric.file_name} {group_measurement.metric} {group_values} -> {group_measurement.value}')
                self.measurements.append(group_measurement)

            sql_metric_tests = sql_metric.tests
            test_variables = {
                measurement.metric: measurement.value
                for measurement in self.scan_result.measurements
                if measurement.column_name is None
            }
            test_variables.update(field_values)
            sql_metric_test_results = self.execute_tests(sql_metric_tests, test_variables, group_values)
            self.scan_result.test_results.extend(sql_metric_test_results)

    def run_sql_metric_default_and_run_tests(self, sql_metric: SqlMetricYml, resolved_sql: AnyStr):
        row_tuple, description = self.warehouse.sql_fetchone_description(resolved_sql)
        test_variables = {
            measurement.metric: measurement.value
            for measurement in self.scan_result.measurements
            if measurement.column_name is None
        }

        for i in range(len(row_tuple)):
            metric_name = description[i][0]
            metric_value = row_tuple[i]
            logging.debug(f'SQL metric {sql_metric.file_name} {metric_name} -> {metric_value}')
            measurement = Measurement(metric=metric_name, value=metric_value)
            test_variables[metric_name] = metric_value
            self.measurements.append(measurement)

        sql_metric_test_results = self.execute_tests(sql_metric.tests, test_variables)
        self.scan_result.test_results.extend(sql_metric_test_results)

    def run_table_tests(self):
        test_variables = {
            measurement.metric: measurement.value
            for measurement in self.scan_result.measurements
            if measurement.column_name is None
        }

        table_tests = self.scan_yml.tests
        table_test_results = self.execute_tests(table_tests, test_variables)
        self.scan_result.test_results.extend(table_test_results)

    def run_column_tests(self):
        for column_name_lower in self.scan_columns:
            scan_column: ScanColumn = self.scan_columns[column_name_lower]
            column_tests = scan_column.get_tests()
            test_variables = {
                measurement.metric: measurement.value
                for measurement in self.scan_result.measurements
                if measurement.column_name is None or measurement.column_name.lower() == column_name_lower
            }
            column_test_results = self.execute_tests(column_tests, test_variables)
            self.scan_result.test_results.extend(column_test_results)

    def execute_tests(self, tests, variables, group_values: Optional[dict] = None):
        test_results = []
        if tests:
            for test in tests:
                test_results.append(test.evaluate(variables, group_values))
        return test_results

    def add_query(self, measurement: Measurement):
        return self.add_measurement('Query', measurement)

    def add_derived(self, measurement: Measurement):
        return self.add_measurement('Derived', measurement)

    def add_measurement(self, measurement_type: str, measurement: Measurement):
        self.measurements.append(measurement)
        logging.debug(f'{measurement_type} measurement: {measurement}')

    def flush_measurements(self):
        if len(self.measurements) > 0:
            self.scan_result.measurements.extend(self.measurements)
            if self.soda_client:
                measurements_json = [measurement.to_json() for measurement in self.measurements]
                self.soda_client.send_measurements(self.scan_reference, measurements_json)
            self.measurements = []
