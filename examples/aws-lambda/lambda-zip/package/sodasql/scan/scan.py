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
import tempfile
from datetime import datetime
from math import floor, ceil
from typing import List, Optional

from jinja2 import Template

from sodasql.scan.column_metadata import ColumnMetadata
from sodasql.scan.group_value import GroupValue
from sodasql.scan.measurement import Measurement
from sodasql.scan.metric import Metric
from sodasql.scan.scan_column import ScanColumn
from sodasql.scan.scan_error import SodaCloudScanError, ScanError, TestExecutionScanError
from sodasql.scan.scan_result import ScanResult
from sodasql.scan.scan_yml import ScanYml
from sodasql.scan.sql_metric_yml import SqlMetricYml
from sodasql.scan.test_result import TestResult
from sodasql.scan.warehouse import Warehouse
from sodasql.soda_server_client.soda_server_client import SodaServerClient


class Scan:

    def __init__(self,
                 warehouse: Warehouse,
                 scan_yml: ScanYml = None,
                 soda_server_client: SodaServerClient = None,
                 variables: dict = None,
                 time: str = None):
        self.warehouse = warehouse
        self.scan_yml = scan_yml
        self.soda_server_client = soda_server_client
        self.variables = variables
        self.time = time

        self.scan_result = ScanResult()
        self.dialect = warehouse.dialect
        self.qualified_table_name = self.dialect.qualify_table_name(scan_yml.table_name)
        self.scan_reference = None
        self.column_metadatas: List[ColumnMetadata] = []
        self.column_names: List[str] = []
        # maps column names (lower case) to ScanColumn's
        self.scan_columns: dict = {}
        self.close_warehouse = True
        self.send_scan_end = True
        self.start_time = None
        self.queries_executed = 0
        self.sampler = None

        self.table_sample_clause = \
            f'\nTABLESAMPLE {scan_yml.sample_method}({scan_yml.sample_percentage})' \
                if scan_yml.sample_percentage \
                else ''

        self.filter_sql = None
        if scan_yml.filter_template:
            if not variables:
                raise RuntimeError(f'No variables provided while filter "{str(scan_yml.filter)}" specified')
            self.filter_sql = scan_yml.filter_template.render(variables)

    def execute(self) -> ScanResult:
        self.start_time = datetime.now()
        if self.soda_server_client:
            logging.debug(f'Soda cloud: {self.soda_server_client.host}')
            self._ensure_scan_reference()

        from sodasql.scan.sampler import Sampler
        self.sampler = Sampler(self)

        try:
            if self.scan_yml:
                # Soda Server and the code below require that the schema measurements is the first measurement
                self._query_columns_metadata()
                self._query_aggregations()
                self._query_group_by_value()
                self._query_histograms()

            self._query_sql_metrics_and_run_tests()
            self._process_cloud_custom_metrics()
            self._run_table_tests()
            self._run_column_tests()
            self._take_samples()

            logging.debug(f'Executed {self.queries_executed} queries in {(datetime.now() - self.start_time)}')

        except Exception as e:
            logging.exception('Exception during scan')
            self.scan_result.add_error(ScanError('Exception during scan', e))

        finally:
            if self.soda_server_client and self.send_scan_end:
                try:
                    self.soda_server_client.scan_ended(self.scan_reference, self.scan_result.errors)
                except Exception as e:
                    logging.error(f'Soda Cloud error: Could not end scan: {e}')
                    self.scan_result.add_error(SodaCloudScanError('Could not end scan', e))

            if self.close_warehouse:
                self.warehouse.close()

        return self.scan_result

    def _process_cloud_custom_metrics(self):
        if self.soda_server_client:
            from sodasql.soda_server_client.monitor_metric_parser import MonitorMetricParser
            from sodasql.soda_server_client.monitor_metric import MonitorMetricType
            custom_metrics = []
            try:
                logging.debug(f'Fetching custom metrics with scanReference: {self.scan_reference}')
                custom_metrics = self.soda_server_client.custom_metrics(self.scan_reference)
            except Exception as e:
                logging.error(f'Soda cloud error: Could not fetch custom_metrics: {e}')
                self.scan_result.add_error(SodaCloudScanError('Could not fetch custom metrics', e))
            if custom_metrics:
                for monitor_metric_dict in custom_metrics:
                    monitor_metric_parser = MonitorMetricParser(monitor_metric_dict, self)
                    if not monitor_metric_parser.has_warnings_or_errors():
                        monitor_metric = monitor_metric_parser.monitor_metric
                        monitor_measurement = monitor_metric.execute()
                        self.scan_result.measurements.append(monitor_measurement)
                        monitor_measurement_json = monitor_measurement.to_json()
                        self.soda_server_client.scan_monitor_measurements(self.scan_reference,
                                                                          monitor_measurement_json)
                        if monitor_metric.metric_type in [MonitorMetricType.MISSING_VALUES_COUNT,
                                                          MonitorMetricType.MISSING_VALUES_PERCENTAGE,
                                                          MonitorMetricType.INVALID_VALUES_COUNT,
                                                          MonitorMetricType.INVALID_VALUES_PERCENTAGE,
                                                          MonitorMetricType.DISTINCT_VALUES_COUNT,
                                                          ]:
                            logging.debug(
                                f'Sending failed rows for {monitor_metric.metric_type} to Soda Cloud')
                            self._send_failed_rows_custom_metric(metric_name="custom_metric",
                                                                 sql=monitor_metric.sql,
                                                                 column_name=monitor_metric.column_name,
                                                                 metric_id=monitor_metric.metric_id)
                        else:
                            logging.debug(
                                f'{monitor_metric.metric_type} is not "negative values metric", failed rows are not sent to Soda Cloud')

    def _query_columns_metadata(self):
        sql = self.warehouse.dialect.sql_columns_metadata_query(self.scan_yml.table_name)
        column_tuples = self.warehouse.sql_fetchall(sql) if sql != '' else self.warehouse.dialect.sql_columns_metadata(
            self.scan_yml.table_name)
        self.queries_executed += 1

        self.column_metadatas = []
        for column_tuple in column_tuples:
            name = column_tuple[0]
            data_type = column_tuple[1]
            nullable = 'YES' == column_tuple[2].upper()
            self.column_metadatas.append(ColumnMetadata(name=name, data_type=data_type, nullable=nullable))

        self.scan_columns: dict = {}
        self.column_names: List[str] = [column_metadata.name for column_metadata in self.column_metadatas]
        for column_metadata in self.column_metadatas:
            scan_column = ScanColumn(self, column_metadata)
            if scan_column.is_supported:
                if scan_column.is_numeric:
                    column_metadata.semantic_type = 'number'
                elif scan_column.is_temporal:
                    column_metadata.semantic_type = 'time'
                # WARN: Order of conditions is important here, because the is_text is based on database data type,
                # and is_numeric, is_temporal are calculated based on validity format. So both is_text and
                # (is_temporal|is_numeric) can be true.
                elif scan_column.is_text:
                    column_metadata.semantic_type = 'text'

                logging.debug(f'  {scan_column.column_name} ({scan_column.column.data_type}) '
                              f'{"" if scan_column.column.nullable else "not null"}')
                self.scan_columns[column_metadata.name.lower()] = scan_column
            else:
                logging.error(f'  {scan_column.column_name} ({scan_column.column.data_type}) -> unsupported, skipped!')

        logging.debug(str(len(self.column_metadatas)) + ' columns:')

        # Compare the column names in yml with valid column names from the table
        invalid_column_names = set(self.scan_yml.columns.keys()) - set(self.scan_columns.keys())
        if invalid_column_names:
            logging.error(f'Unknown column names found in YML: {", ".join(invalid_column_names)} \n'
                          f'Scan will continue for known columns.')

        schema_measurement_value = [column_metadata.to_json() for column_metadata in self.column_metadatas]
        schema_measurement = Measurement(Metric.SCHEMA, value=schema_measurement_value)
        self._log_measurement(schema_measurement)
        self._flush_measurements([schema_measurement])

    def _query_aggregations(self):
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

        try:
            for column_name_lower, scan_column in self.scan_columns.items():
                metric_indices = {}
                column_metric_indices[column_name_lower] = metric_indices
                column_name = scan_column.column_name

                if scan_column.is_missing_enabled:
                    metric_indices['non_missing'] = len(measurements)
                    if scan_column.non_missing_condition:
                        fields.append(dialect.sql_expr_count_conditional(scan_column.non_missing_condition))
                    else:
                        fields.append(dialect.sql_expr_count(scan_column.qualified_column_name))
                    measurements.append(Measurement(Metric.VALUES_COUNT, column_name))

                if scan_column.is_valid_enabled:
                    metric_indices['valid'] = len(measurements)
                    if scan_column.non_missing_and_valid_condition:
                        fields.append(dialect.sql_expr_count_conditional(scan_column.non_missing_and_valid_condition))
                    else:
                        fields.append(dialect.sql_expr_count(scan_column.qualified_column_name))
                    measurements.append(Measurement(Metric.VALID_COUNT, column_name))

                if scan_column.is_text:
                    length_expr = dialect.sql_expr_conditional(
                        scan_column.non_missing_and_valid_condition,
                        dialect.sql_expr_length(scan_column.qualified_column_name)) \
                        if scan_column.non_missing_and_valid_condition \
                        else dialect.sql_expr_length(scan_column.qualified_column_name)

                    if self.scan_yml.is_metric_enabled(Metric.AVG_LENGTH, column_name):
                        fields.append(dialect.sql_expr_avg(length_expr))
                        measurements.append(Measurement(Metric.AVG_LENGTH, column_name))

                    if self.scan_yml.is_metric_enabled(Metric.MIN_LENGTH, column_name):
                        fields.append(dialect.sql_expr_min(length_expr))
                        measurements.append(Measurement(Metric.MIN_LENGTH, column_name))

                    if self.scan_yml.is_metric_enabled(Metric.MAX_LENGTH, column_name):
                        fields.append(dialect.sql_expr_max(length_expr))
                        measurements.append(Measurement(Metric.MAX_LENGTH, column_name))

                if scan_column.is_numeric:
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
                if self.filter_sql:
                    sql += f'\nWHERE {self.filter_sql}'

                query_result_tuple = self.warehouse.sql_fetchone(sql)
                self.queries_executed += 1

                for i in range(0, len(measurements)):
                    measurement = measurements[i]
                    measurement.value = query_result_tuple[i]
                    self._log_measurement(measurement)

                # Calculating derived measurements
                row_count_measurement = next((m for m in measurements if m.metric == Metric.ROW_COUNT), None)
                if row_count_measurement:
                    row_count = row_count_measurement.value
                    for column_name_lower, scan_column in self.scan_columns.items():
                        column_name = scan_column.column_name
                        metric_indices = column_metric_indices[column_name_lower]
                        non_missing_index = metric_indices.get('non_missing')
                        if non_missing_index is not None:
                            values_count = measurements[non_missing_index].value
                            missing_count = row_count - values_count
                            missing_percentage = missing_count * 100 / row_count if row_count > 0 else None
                            values_percentage = values_count * 100 / row_count if row_count > 0 else None

                            self._log_and_append_derived_measurements(measurements, [
                                Measurement(Metric.MISSING_PERCENTAGE, column_name, missing_percentage),
                                Measurement(Metric.MISSING_COUNT, column_name, missing_count),
                                Measurement(Metric.VALUES_PERCENTAGE, column_name, values_percentage)
                            ])

                            valid_index = metric_indices.get('valid')
                            if valid_index is not None:
                                valid_count = measurements[valid_index].value
                                invalid_count = row_count - missing_count - valid_count
                                invalid_percentage = invalid_count * 100 / row_count if row_count > 0 else None
                                valid_percentage = valid_count * 100 / row_count if row_count > 0 else None
                                self._log_and_append_derived_measurements(measurements, [
                                    Measurement(Metric.INVALID_PERCENTAGE, column_name, invalid_percentage),
                                    Measurement(Metric.INVALID_COUNT, column_name, invalid_count),
                                    Measurement(Metric.VALID_PERCENTAGE, column_name, valid_percentage)
                                ])

            self._flush_measurements(measurements)
        except Exception as e:
            logging.debug(f'Exception during aggregation query', e)
            self.scan_result.add_error(ScanError(f'Exception during aggregation query', e))

    def _query_group_by_value(self):
        for column_name_lower, scan_column in self.scan_columns.items():
            try:
                measurements = []

                column_name = scan_column.column_name

                if scan_column.is_any_metric_enabled(
                    [Metric.DISTINCT, Metric.UNIQUENESS, Metric.UNIQUE_COUNT,
                     Metric.MINS, Metric.MAXS, Metric.FREQUENT_VALUES, Metric.DUPLICATE_COUNT]):

                    group_by_cte = scan_column.get_group_by_cte()
                    numeric_value_expr = scan_column.get_group_by_cte_numeric_value_expression()
                    order_by_value_expr = scan_column.get_order_by_cte_value_expression(numeric_value_expr)

                    if self.scan_yml.is_any_metric_enabled(
                        [Metric.DISTINCT, Metric.UNIQUENESS, Metric.UNIQUE_COUNT, Metric.DUPLICATE_COUNT],
                        column_name):

                        sql = (f'{group_by_cte} \n'
                               f'SELECT COUNT(*), \n'
                               f'       COUNT(CASE WHEN frequency = 1 THEN 1 END), \n'
                               f'       SUM(frequency) \n'
                               f'FROM group_by_value')

                        query_result_tuple = self.warehouse.sql_fetchone(sql)
                        self.queries_executed += 1

                        distinct_count = query_result_tuple[0]
                        unique_count = query_result_tuple[1]
                        valid_count = query_result_tuple[2] if query_result_tuple[2] else 0
                        duplicate_count = distinct_count - unique_count

                        self._log_and_append_query_measurement(
                            measurements, Measurement(Metric.DISTINCT, column_name, distinct_count))
                        self._log_and_append_query_measurement(
                            measurements, Measurement(Metric.UNIQUE_COUNT, column_name, unique_count))

                        derived_measurements = [Measurement(Metric.DUPLICATE_COUNT, column_name, duplicate_count)]
                        if valid_count > 1:
                            uniqueness = (distinct_count - 1) * 100 / (valid_count - 1)
                            derived_measurements.append(Measurement(Metric.UNIQUENESS, column_name, uniqueness))
                        self._log_and_append_derived_measurements(measurements, derived_measurements)

                    if scan_column.is_metric_enabled(Metric.MINS) and order_by_value_expr:
                        sql = (f'{group_by_cte} \n'
                               f'SELECT value \n'
                               f'FROM group_by_value \n'
                               f'ORDER BY {order_by_value_expr} ASC \n'
                               f'{self.dialect.sql_expr_limit(scan_column.mins_maxs_limit)}\n')

                        rows = self.warehouse.sql_fetchall(sql)
                        self.queries_executed += 1

                        mins = [row[0] for row in rows]
                        self._log_and_append_query_measurement(measurements,
                                                               Measurement(Metric.MINS, column_name, mins))

                    if self.scan_yml.is_metric_enabled(Metric.MAXS, column_name) and order_by_value_expr:
                        sql = (f'{group_by_cte} \n'
                               f'SELECT value \n'
                               f'FROM group_by_value \n'
                               f'ORDER BY {order_by_value_expr} DESC \n'
                               f'{self.dialect.sql_expr_limit(scan_column.mins_maxs_limit)}\n')

                        rows = self.warehouse.sql_fetchall(sql)
                        self.queries_executed += 1

                        maxs = [row[0] for row in rows]
                        self._log_and_append_query_measurement(measurements,
                                                               Measurement(Metric.MAXS, column_name, maxs))

                    if self.scan_yml.is_metric_enabled(Metric.FREQUENT_VALUES, column_name):
                        frequent_values_limit = self.scan_yml.get_frequent_values_limit(column_name)
                        sql = (f'{group_by_cte} \n'
                               f'SELECT value, frequency \n'
                               f'FROM group_by_value \n'
                               f'ORDER BY frequency DESC \n'
                               f'{self.dialect.sql_expr_limit(scan_column.mins_maxs_limit)}\n')

                        rows = self.warehouse.sql_fetchall(sql)
                        self.queries_executed += 1

                        frequent_values = [{'value': row[0], 'frequency': row[1]} for row in rows]
                        self._log_and_append_query_measurement(
                            measurements, Measurement(Metric.FREQUENT_VALUES, column_name, frequent_values))

                self._flush_measurements(measurements)
            except Exception as e:
                self.scan_result.add_error(ScanError(f'Exception during column group by value queries', e))

    def _query_histograms(self):

        for column_name_lower, scan_column in self.scan_columns.items():
            column_name = scan_column.column_name
            try:
                if scan_column.is_metric_enabled(Metric.HISTOGRAM) and scan_column.numeric_expr:
                    measurements = []
                    buckets: int = scan_column.get_histogram_buckets()

                    min_value = scan_column.get_metric_value(Metric.MIN)
                    max_value = scan_column.get_metric_value(Metric.MAX)

                    if scan_column.is_numeric and min_value and max_value and min_value < max_value:
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
                            field_clauses.append(
                                f'SUM(CASE WHEN {lower_bound}{optional_and}{upper_bound} THEN frequency END)')

                        fields = ',\n  '.join(field_clauses)

                        sql = (f'{group_by_cte} \n'
                               f'SELECT \n'
                               f'  {fields} \n'
                               f'FROM group_by_value')

                        row = self.warehouse.sql_fetchone(sql)
                        self.queries_executed += 1

                        # Process the histogram query
                        frequencies = []
                        for i in range(0, buckets):
                            frequency = row[i]
                            frequencies.append(0 if not frequency else int(frequency))
                        histogram = {
                            'boundaries': boundaries,
                            'frequencies': frequencies
                        }

                        self._log_and_append_query_measurement(measurements,
                                                               Measurement(Metric.HISTOGRAM, column_name, histogram))
                        self._flush_measurements(measurements)
            except Exception as e:
                self.scan_result.add_error(ScanError(f'Exception during histogram query for {column_name}', e))

    def _query_sql_metrics_and_run_tests(self):
        self._query_sql_metrics_and_run_tests_base(self.scan_yml.sql_metric_ymls)
        for column_name_lower, scan_column in self.scan_columns.items():
            if scan_column and scan_column.scan_yml_column:
                self._query_sql_metrics_and_run_tests_base(scan_column.scan_yml_column.sql_metric_ymls, scan_column)

    def _query_sql_metrics_and_run_tests_base(self,
                                              sql_metric_ymls: Optional[List[SqlMetricYml]],
                                              scan_column: Optional[ScanColumn] = None):
        if sql_metric_ymls:
            for sql_metric in sql_metric_ymls:
                try:
                    resolved_sql = self.resolve_sql_metric_sql(sql_metric)

                    if sql_metric.type == 'numeric':
                        self._run_sql_metric_default_and_run_tests(sql_metric, resolved_sql, scan_column)
                    elif sql_metric.type == 'numeric_groups':
                        self._run_sql_metric_with_groups_and_run_tests(sql_metric, resolved_sql, scan_column)
                    elif sql_metric.type == 'failed_rows':
                        self._run_sql_metric_failed_rows(sql_metric, resolved_sql, scan_column)
                except Exception as e:
                    msg = f"Couldn't run sql metric {sql_metric}: {e}"
                    logging.exception(msg)
                    self.scan_result.add_error(ScanError(f'Could not run sql metric {sql_metric}', e))

    def resolve_sql_metric_sql(self, sql_metric):
        if self.variables:
            sql_variables = self.variables.copy() if self.variables else {}
            template = Template(sql_metric.sql)
            resolved_sql = template.render(sql_variables)
        else:
            resolved_sql = sql_metric.sql
        return resolved_sql

    def _run_sql_metric_default_and_run_tests(self,
                                              sql_metric: SqlMetricYml,
                                              resolved_sql: str,
                                              scan_column: Optional[ScanColumn] = None):
        try:
            row_tuple, description = self.warehouse.sql_fetchone_description(resolved_sql)
            self.queries_executed += 1

            test_variables = self._get_test_variables(scan_column)
            column_name = scan_column.column_name if scan_column is not None else None

            measurements = []
            for i in range(len(row_tuple)):
                metric_name = sql_metric.metric_names[i] if sql_metric.metric_names is not None else description[i][0]
                metric_value = row_tuple[i]
                logging.debug(f'SQL metric {sql_metric.title} {metric_name} -> {metric_value}')
                measurement = Measurement(metric=metric_name, value=metric_value, column_name=column_name)
                test_variables[metric_name] = metric_value
                self._log_and_append_query_measurement(measurements, measurement)

            self._flush_measurements(measurements)

            sql_metric_test_results = self._execute_tests(sql_metric.tests, test_variables)
            self._flush_test_results(sql_metric_test_results)
        except Exception as e:
            self.scan_result.add_error(ScanError(f'Exception during sql metric query {resolved_sql}', e))

    def _run_sql_metric_with_groups_and_run_tests(self,
                                                  sql_metric: SqlMetricYml,
                                                  resolved_sql: str,
                                                  scan_column: Optional[ScanColumn]):
        try:
            measurements = []
            test_results = []
            group_fields_lower = set(group_field.lower() for group_field in sql_metric.group_fields)

            rows, description = self.warehouse.sql_fetchall_description(resolved_sql)
            self.queries_executed += 1

            group_values_by_metric_name = {}
            for row in rows:
                group = {}
                metric_values = {}

                for i in range(len(row)):
                    metric_name = sql_metric.metric_names[i] if sql_metric.metric_names is not None else description[i][
                        0]
                    metric_value = row[i]
                    if metric_name.lower() in group_fields_lower:
                        group[metric_name] = metric_value
                    else:
                        metric_values[metric_name] = metric_value

                if not group:
                    logging.error(f'None of the declared group_fields were found in '
                                  f'result: {sql_metric.group_fields}. Skipping result.')
                else:
                    for metric_name in metric_values:
                        metric_value = metric_values[metric_name]
                        if metric_name not in group_values_by_metric_name:
                            group_values_by_metric_name[metric_name] = []
                        group_values = group_values_by_metric_name[metric_name]
                        group_values.append(GroupValue(group=group, value=metric_value))
                        logging.debug(f'SQL metric {sql_metric.title} {metric_name} {group} -> {metric_value}')

                    sql_metric_tests = sql_metric.tests
                    test_variables = self._get_test_variables(scan_column)
                    test_variables.update(metric_values)
                    sql_metric_test_results = self._execute_tests(sql_metric_tests, test_variables, group)
                    test_results.extend(sql_metric_test_results)

            column_name = scan_column.column_name if scan_column is not None else None
            for metric_name in group_values_by_metric_name:
                group_values = group_values_by_metric_name[metric_name]
                measurement = Measurement(metric=metric_name, group_values=group_values, column_name=column_name)
                self._log_and_append_query_measurement(measurements, measurement)

            self._flush_measurements(measurements)
            self._flush_test_results(test_results)
        except Exception as e:
            self.scan_result.add_error(ScanError(f'Exception during sql metric groups query {resolved_sql}', e))

    def _send_failed_rows_custom_metric(self,
                                        metric_name: str,
                                        column_name: str,
                                        sql: str,
                                        metric_id: str):
        try:
            if self.soda_server_client:

                logging.debug(f'Sending failed rows for {metric_id} to Soda Cloud')
                with tempfile.TemporaryFile() as temp_file:

                    failed_limit = 5  # TODO: default for custom metrics

                    stored_failed_rows, sample_columns, total_failed_rows = \
                        self.sampler.save_sample_to_local_file_with_limit(sql, temp_file, failed_limit)

                    if stored_failed_rows > 0:
                        temp_file_size_in_bytes = temp_file.tell()
                        temp_file.seek(0)

                        file_path = self.sampler.create_file_path_failed_rows_sql_metric(column_name=column_name,
                                                                                         metric_name=metric_name)
                        file_id = self.soda_server_client.scan_upload(self.scan_reference,
                                                                      file_path,
                                                                      temp_file,
                                                                      temp_file_size_in_bytes)

                        self.soda_server_client.scan_file(
                            scan_reference=self.scan_reference,
                            sample_type='failedRowsSample',
                            stored=int(stored_failed_rows),
                            total=int(total_failed_rows),
                            source_columns=sample_columns,
                            file_id=file_id,
                            column_name=column_name,
                            test_ids=[],
                            sql_metric_name=metric_name,
                            custom_metric_id=metric_id)
                        logging.debug(
                            f'Sent failed rows for id: {metric_id} ({stored_failed_rows}/{total_failed_rows}) to Soda Cloud')
                    else:
                        logging.debug(f'No failed rows for custom metric ({metric_name} with id {metric_id})')
            else:
                failed_rows_tuples, description = self.warehouse.sql_fetchall_description(sql=sql)
                if len(failed_rows_tuples) > 0:
                    table_text = self._table_to_text(failed_rows_tuples, description)
                    logging.debug(f'Failed rows for sql metric sql {metric_name}:\n' + table_text)
                else:
                    logging.debug(f'No failed rows for custom metric ({metric_name} with id {metric_id})')
        except Exception as e:
            logging.exception(f'Could not perform sql metric failed rows \n{sql}', e)
            self.scan_result.add_error(ScanError(f'Exception during sql metric failed rows query {sql}', e))

    def _run_sql_metric_failed_rows(self,
                                    sql_metric_yml: SqlMetricYml,
                                    resolved_sql: str,
                                    scan_column: Optional[ScanColumn] = None):

        try:
            if self.soda_server_client:

                logging.debug(f'Sending failed rows for sql metric {sql_metric_yml.name} to Soda Cloud')
                with tempfile.TemporaryFile() as temp_file:

                    failed_limit = self.scan_yml.get_sql_metric_failed_rows_limit(sql_metric_yml)

                    stored_failed_rows, sample_columns, total_failed_rows = \
                        self.sampler.save_sample_to_local_file_with_limit(resolved_sql, temp_file, failed_limit)

                    column_name = scan_column.column_name if scan_column else None
                    measurement = Measurement(metric=sql_metric_yml.name, value=total_failed_rows,
                                              column_name=column_name)

                    measurements = []
                    self._log_and_append_query_measurement(measurements, measurement)
                    self._flush_measurements(measurements)

                    test = sql_metric_yml.tests[0]

                    test_variables = {
                        sql_metric_yml.name: total_failed_rows
                    }

                    test_result = self.evaluate_test(test, test_variables)
                    self._flush_test_results([test_result])

                    if stored_failed_rows > 0:
                        temp_file_size_in_bytes = temp_file.tell()
                        temp_file.seek(0)

                        file_path = self.sampler.create_file_path_failed_rows_sql_metric(
                            column_name=sql_metric_yml.column_name,
                            metric_name=sql_metric_yml.name)

                        file_id = self.soda_server_client.scan_upload(self.scan_reference,
                                                                      file_path,
                                                                      temp_file,
                                                                      temp_file_size_in_bytes)

                        self.soda_server_client.scan_file(
                            scan_reference=self.scan_reference,
                            sample_type='failedRowsSample',
                            stored=int(stored_failed_rows),
                            total=int(total_failed_rows),
                            source_columns=sample_columns,
                            file_id=file_id,
                            column_name=sql_metric_yml.column_name,
                            test_ids=[test.id],
                            sql_metric_name=sql_metric_yml.name)
                        logging.debug(
                            f'Sent failed rows for sql metric ({stored_failed_rows}/{total_failed_rows}) to Soda Cloud')
                    else:
                        logging.debug(f'No failed rows for sql metric ({sql_metric_yml.name})')
            else:
                failed_rows_tuples, description = self.warehouse.sql_fetchall_description(sql=resolved_sql)
                if len(failed_rows_tuples) > 0:
                    table_text = self._table_to_text(failed_rows_tuples, description)
                    logging.debug(f'Failed rows for sql metric sql {sql_metric_yml.name}:\n' + table_text)
                else:
                    logging.debug(f'No failed rows for sql metric sql {sql_metric_yml.name}')
        except Exception as e:
            logging.exception(f'Could not perform sql metric failed rows \n{resolved_sql}', e)
            self.scan_result.add_error(ScanError(f'Exception during sql metric failed rows query {resolved_sql}', e))

    def _get_test_variables(self, scan_column: Optional[ScanColumn] = None):
        column_name_lower = scan_column.column_name_lower if scan_column is not None else None
        return {
            measurement.metric: measurement.value
            for measurement in self.scan_result.measurements
            if measurement.column_name is None or measurement.column_name.lower() == column_name_lower
        }

    def _run_table_tests(self):
        test_variables = self._get_test_variables()

        table_tests = self.scan_yml.tests
        table_test_results = self._execute_tests(table_tests, test_variables)
        self._flush_test_results(table_test_results)

    def _run_column_tests(self):
        test_results = []
        for column_name_lower, scan_column in self.scan_columns.items():
            column_tests = scan_column.get_tests()
            test_variables = self._get_test_variables(scan_column)
            column_test_results = self._execute_tests(column_tests, test_variables)
            test_results.extend(column_test_results)
        self._flush_test_results(test_results)

    def _execute_tests(self, tests, variables, group_values: Optional[dict] = None):
        test_results = []
        if tests:
            for test in tests:
                test_result = self.evaluate_test(test, variables, group_values)
                test_results.append(test_result)
        return test_results

    # noinspection PyTypeChecker
    def _take_samples(self):
        if self.soda_server_client:
            try:
                for measurement in self.scan_result.measurements:
                    if (measurement.metric in [Metric.ROW_COUNT, Metric.MISSING_COUNT, Metric.INVALID_COUNT,
                                               Metric.VALUES_COUNT, Metric.VALID_COUNT]
                        and measurement.value > 0):
                        samples_yml = self.scan_yml.get_sample_yml(measurement)
                        if samples_yml:
                            self.sampler.save_sample(samples_yml, measurement, self.scan_result.test_results)
            except Exception as e:
                logging.exception(f'Soda cloud error: Could not upload samples: {e}')
                self.scan_result.add_error(SodaCloudScanError('Could not upload samples', e))

    @classmethod
    def _log_and_append_query_measurement(cls, measurements: List[Measurement], measurement: Measurement):
        """
        Convenience method to log a measurement and append it to the given list of measurements
        Logging will indicate it is the result of a query
        """
        cls._log_measurement(measurement)
        measurements.append(measurement)

    @classmethod
    def _log_and_append_derived_measurements(
        cls,
        measurements: List[Measurement],
        derived_measurements: List[Measurement]):
        """
        Convenience method to log a list of derived measurements and append them to the given list of measurements.
        Logging will indicate it is a derived measurement
        """
        for derived_measurement in derived_measurements:
            cls._log_measurement(derived_measurement, is_derived=True)
            measurements.append(derived_measurement)

    @classmethod
    def _log_measurement(cls, measurement, is_derived: bool = False):
        measurement_type = "Derived" if is_derived else "Query"
        logging.debug(f'{measurement_type} measurement: {measurement}')

    def _flush_measurements(self, measurements: List[Measurement]):
        """
        Adds the measurements to the scan result and sends the measurements to the Soda Server if that's configured
        """
        self.scan_result.measurements.extend(measurements)
        if self.soda_server_client and measurements:
            measurement_jsons = [measurement.to_json() for measurement in measurements]
            try:
                self.soda_server_client.scan_measurements(self.scan_reference, measurement_jsons)
            except Exception as e:
                logging.error(f'Soda Cloud error: Could not send measurements: {e}')
                self.scan_result.add_error(SodaCloudScanError('Could not send measurements', e))

    def _flush_test_results(self, test_results: List[TestResult]):
        """
        Adds the test_results to the scan result and sends the measurements to the Soda Server if that's configured
        """
        if test_results:
            self.scan_result.add_test_results(test_results)

            if self.soda_server_client:
                test_result_jsons = [test_result.to_json() for test_result in test_results]
                try:
                    self.soda_server_client.scan_test_results(self.scan_reference, test_result_jsons)
                except Exception as e:
                    logging.error(f'Soda Cloud error: Could not send test results: {e}')
                    self.scan_result.add_error(SodaCloudScanError('Could not send test results', e))

    def _ensure_scan_reference(self):
        if self.soda_server_client and not self.scan_reference:
            try:
                self.start_scan_response = self.soda_server_client.scan_start(
                    self.warehouse,
                    self.scan_yml,
                    self.time)
                self.scan_reference = self.start_scan_response['scanReference']
            except Exception as e:
                logging.error(f'Soda Cloud error: Could not start scan: {e}')
                logging.error(f'Skipping subsequent Soda Cloud communication but continuing the scan')
                self.scan_result.add_error(SodaCloudScanError('Could not start scan', e))
                self.soda_server_client = None

    def evaluate_test(self, test, test_variables, group_values: Optional[dict] = None) -> TestResult:
        test_result = test.evaluate(test_variables, group_values)
        if test_result.error:
            self.scan_result.add_error(TestExecutionScanError(
                message=f'Test "{test_result.test.expression}" failed',
                exception=test_result.error,
                test=test_result.test
            ))
        return test_result

    def _table_to_text(self, failed_row_tuples: tuple, description):
        columns = [col[0] for col in description]
        widths = [len(col) for col in columns]
        tavnit = '|'
        separator = '+'

        for failed_row_tuple in failed_row_tuples:
            failed_row_tuple = tuple(str(cell) for cell in failed_row_tuple)
            for i in range(0, len(failed_row_tuple)):
                widths[i] = max(widths[i], len(failed_row_tuple[i]))

        for w in widths:
            tavnit += " %-" + "%ss |" % (w,)
            separator += '-' * w + '--+'

        table_text = ''
        table_text += separator + '\n'
        table_text += tavnit % tuple(columns) + '\n'
        table_text += separator + '\n'
        for row in failed_row_tuples:
            table_text += tavnit % row + '\n'
        table_text += separator
        return table_text
