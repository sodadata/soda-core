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

from sodatools.scan.column import Column
from sodatools.scan.parse_logs import ParseLogs
from sodatools.scan.valid_format import VALID_FORMATS


class ColumnConfiguration:

    VALID_KEYS = ['metrics', 'missing_values', 'valid_format', 'valid_regex']

    def __init__(self, column_name: str, column_dict: dict, parse_logs: ParseLogs):
        self.metrics = column_dict.get('metrics', [])
        self.missing_values = column_dict.get('missing_values', [])
        self.valid_format = column_dict.get('valid_format')
        self.valid_regex = column_dict.get('valid_regex')

        parse_logs.warning_invalid_elements(
            self.metrics,
            ScanConfiguration.METRIC_TYPES,
            f'Invalid columns.{column_name} metric')

        parse_logs.warning_invalid_elements(
            column_dict.keys(),
            ColumnConfiguration.VALID_KEYS,
            f'Invalid key in columns.{column_name}')


class ScanConfiguration:

    VALID_KEYS = ['table_name', 'column_metrics', 'columns', 'sample_size']

    METRIC_MISSING = 'missing'
    METRIC_INVALID = 'invalid'
    METRIC_MIN = 'min'
    METRIC_MAX = 'max'
    METRIC_AVG = 'avg'
    METRIC_SUM = 'sum'
    METRIC_MIN_LENGTH = 'min_length'
    METRIC_MAX_LENGTH = 'max_length'
    METRIC_AVG_LENGTH = 'avg_length'
    METRIC_DISTINCT = 'distinct'
    METRIC_UNIQUENESS = 'uniqueness'

    METRIC_TYPES = [
        METRIC_MISSING,
        METRIC_INVALID,
        METRIC_MIN,
        METRIC_MAX,
        METRIC_AVG,
        METRIC_SUM,
        METRIC_MIN_LENGTH,
        METRIC_MAX_LENGTH,
        METRIC_AVG_LENGTH,
        METRIC_DISTINCT,
        METRIC_UNIQUENESS
    ]

    def __init__(self, scan_dict: dict):
        self.parse_logs = ParseLogs()
        self.table_name = scan_dict.get('table_name')
        if not self.table_name:
            self.parse_logs.error('table_name is required')
        self.column_metrics = scan_dict.get('column_metrics', [])
        if not isinstance(self.column_metrics, list):
            self.parse_logs.error('column_metrics is not a list')
        else:
            self.parse_logs.warning_invalid_elements(
                self.column_metrics,
                ScanConfiguration.METRIC_TYPES,
                'Invalid column_metrics value')
        self.columns = {}
        columns_dict = scan_dict.get('columns', {})
        for column_name in columns_dict:
            column_dict = columns_dict[column_name]
            column_name_lower = column_name.lower()
            self.columns[column_name_lower] = ColumnConfiguration(column_name, column_dict, self.parse_logs)
        self.sample_size = scan_dict.get('sample_size')
        self.parse_logs.warning_invalid_elements(
            scan_dict.keys(),
            ScanConfiguration.VALID_KEYS,
            'Invalid scan configuration')

    def is_row_count_enabled(self):
        return True

    def is_missing_enabled(self, column):
        return self.__is_metric_enabled(column, self.METRIC_MISSING)

    def is_invalid_enabled(self, column):
        return self.__is_metric_enabled(column, self.METRIC_INVALID)

    def is_min_length_enabled(self, column):
        return self.__is_metric_enabled(column, self.METRIC_MIN_LENGTH)

    def __is_metric_enabled(self, column: Column, metric: str):
        column_configuration = self.columns.get(column.name.lower())
        column_metrics = column_configuration.metrics if column_configuration else None
        return metric in self.column_metrics or (column_metrics and metric in column_metrics)

    def get_missing_values(self, column):
        column_configuration = self.columns.get(column.name.lower())
        missing_values = column_configuration.missing_values if column_configuration else None
        return missing_values if missing_values else []

    def get_valid_regex(self, column):
        column_configuration = self.columns.get(column.name.lower())
        if column_configuration:
            if column_configuration.valid_format:
                regex = VALID_FORMATS.get(column_configuration.valid_format)
                if not regex:
                    logging.warning(f'Invalid valid format for column {column.name}: {column_configuration.valid_format}')
                return regex
            return column_configuration.valid_regex
        return None
