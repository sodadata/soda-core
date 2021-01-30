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
import os
import traceback
from typing import Optional, Set, List, AnyStr

from jinja2 import Template

from sodasql.scan.metric import Metric
from sodasql.scan.missing import Missing
from sodasql.scan.parser import Parser
from sodasql.scan.scan_yml import ScanYml
from sodasql.scan.scan_yml_column import ScanYmlColumn
from sodasql.scan.test import Test
from sodasql.scan.validity import Validity

KEY_TABLE_NAME = 'table_name'
KEY_METRICS = 'metrics'
KEY_TESTS = 'tests'
KEY_COLUMNS = 'columns'
KEY_MINS_MAXS_LIMIT = 'mins_maxs_limit'
KEY_FREQUENT_VALUES_LIMIT = 'frequent_values_limit'
KEY_SAMPLE_PERCENTAGE = 'sample_percentage'
KEY_SAMPLE_METHOD = 'sample_method'
KEY_FILTER = 'filter'

VALID_KEYS = [KEY_TABLE_NAME, KEY_METRICS, KEY_TESTS, KEY_COLUMNS,
              KEY_MINS_MAXS_LIMIT, KEY_FREQUENT_VALUES_LIMIT,
              KEY_SAMPLE_PERCENTAGE, KEY_SAMPLE_METHOD, KEY_FILTER]

COLUMN_KEY_TESTS = 'tests'

COLUMN_KEY_MISSING_VALUES = 'missing_values'
COLUMN_KEY_MISSING_FORMAT = 'missing_format'
COLUMN_KEY_MISSING_REGEX = 'missing_regex'

COLUMN_KEY_VALID_FORMAT = 'valid_format'
COLUMN_KEY_VALID_REGEX = 'valid_regex'
COLUMN_KEY_VALID_VALUES = 'valid_values'
COLUMN_KEY_VALID_MIN = 'valid_min'
COLUMN_KEY_VALID_MAX = 'valid_max'
COLUMN_KEY_VALID_MIN_LENGTH = 'valid_min_length'
COLUMN_KEY_VALID_MAX_LENGTH = 'valid_max_length'

COLUMN_MISSING_KEYS = [
    COLUMN_KEY_MISSING_VALUES,
    COLUMN_KEY_MISSING_FORMAT,
    COLUMN_KEY_MISSING_REGEX]

COLUMN_VALID_KEYS = [
    COLUMN_KEY_VALID_FORMAT,
    COLUMN_KEY_VALID_REGEX,
    COLUMN_KEY_VALID_VALUES,
    COLUMN_KEY_VALID_MIN,
    COLUMN_KEY_VALID_MAX,
    COLUMN_KEY_VALID_MIN_LENGTH,
    COLUMN_KEY_VALID_MAX_LENGTH]

COLUMN_ALL_KEYS = COLUMN_MISSING_KEYS + COLUMN_VALID_KEYS + [
    KEY_METRICS,
    COLUMN_KEY_TESTS]


class ScanYmlParser(Parser):

    def __init__(self,
                 scan_dict: dict,
                 scan_file_name: AnyStr = 'scan'):
        super().__init__(scan_file_name)

        self.scan_yml = ScanYml()

        self._push_context(scan_dict, self.description)

        table_name = self.get_str_required(KEY_TABLE_NAME)
        self.scan_yml.table_name = table_name
        self.scan_yml.metrics = self.parse_metrics()
        self.scan_yml.tests = self.parse_tests(scan_dict, KEY_TESTS, context_table_name=table_name)
        self.scan_yml.columns = self.parse_columns(self.scan_yml)
        self.scan_yml.sample_percentage = self.get_float_optional(KEY_SAMPLE_PERCENTAGE)
        self.scan_yml.sample_method = self.get_str_optional(KEY_SAMPLE_METHOD, 'SYSTEM').upper()
        self.scan_yml.mins_maxs_limit = self.get_int_optional(KEY_MINS_MAXS_LIMIT, 5)
        self.scan_yml.frequent_values_limit = self.get_int_optional(KEY_FREQUENT_VALUES_LIMIT, 5)

        filter = self.get_str_optional(KEY_FILTER)
        if filter:
            try:
                self.scan_yml.filter = filter
                self.scan_yml.filter_template = Template(filter)
            except Exception as e:
                self.error(f"Couldn't parse filter '{filter}': {str(e)}")

        self.check_invalid_keys(VALID_KEYS)

    def parse_metrics(self, column_name: Optional[str] = None):
        configured_metrics_list = self.get_list_optional(KEY_METRICS)
        if configured_metrics_list is None:
            return set()

        if not isinstance(configured_metrics_list, list):
            self.error('metrics is not a list')
            return set()

        metrics: Set[str] = set(configured_metrics_list)

        self._push_context(metrics, KEY_METRICS)

        self.resolve_category(metrics, Metric.CATEGORY_MISSING, Metric.CATEGORY_MISSING_METRICS, column_name)
        self.resolve_category(metrics, Metric.CATEGORY_VALIDITY, Metric.CATEGORY_VALIDITY_METRICS, column_name)
        self.resolve_category(metrics, Metric.CATEGORY_DUPLICATES, Metric.CATEGORY_DUPLICATES_METRICS, column_name)

        if Metric.VALID_COUNT in metrics:
            self.ensure_metric(metrics, Metric.MISSING_COUNT, Metric.CATEGORY_VALIDITY, column_name)
            self.ensure_metric(metrics, Metric.MISSING_PERCENTAGE, Metric.CATEGORY_VALIDITY, column_name)
            self.ensure_metric(metrics, Metric.VALUES_COUNT, Metric.CATEGORY_VALIDITY, column_name)
            self.ensure_metric(metrics, Metric.VALUES_PERCENTAGE, Metric.CATEGORY_VALIDITY, column_name)

        if any(m in metrics for m in Metric.CATEGORY_MISSING_METRICS):
            self.ensure_metric(metrics, Metric.ROW_COUNT, Metric.CATEGORY_MISSING)

        if Metric.HISTOGRAM in metrics:
            self.ensure_metric(metrics, Metric.MIN, Metric.HISTOGRAM, column_name)
            self.ensure_metric(metrics, Metric.MAX, Metric.HISTOGRAM, column_name)

        self.check_invalid_keys(Metric.METRIC_TYPES)

        self._pop_context()

        return set(metrics)

    def ensure_metric(self,
                      metrics: Set[str],
                      metric: str,
                      dependent_metric: str,
                      column_name: str = None):
        if metric not in metrics:
            metrics.add(metric)
            column_message = f' on column {column_name}' if column_name else ''
            self.info(f'Added metric {metric} as dependency of {dependent_metric}{column_message}')

    def is_metric_category_enabled(self, metrics: Set[str], category: str, category_metrics: List[str]):
        if category in metrics:
            return True
        for category_metric in category_metrics:
            if category_metric in metrics:
                return True
        return False

    def resolve_category(self,
                         metrics: Set[str],
                         category: str,
                         category_metrics: List[str],
                         column_name: str = None):
        if self.is_metric_category_enabled(metrics, category, category_metrics):
            if category in metrics:
                metrics.remove(category)
            for category_metric in category_metrics:
                self.ensure_metric(metrics, category_metric, category, column_name)

    def remove_metric(self, metrics: Set[str], metric: str):
        """Returns True if the metric was removed from the set"""
        if metric in metrics:
            metrics.remove(metric)
            return True
        else:
            return False

    def parse_columns(self, scan_configuration: ScanYml) -> dict:
        columns_dict = self.get_dict_optional(KEY_COLUMNS, {})
        self._push_context(columns_dict, KEY_COLUMNS)

        scan_configuration_columns = {}

        for column_name in columns_dict:
            column_dict = columns_dict.get(column_name)
            self._push_context(column_dict, column_name)

            metrics: Set[str] = self.parse_metrics(column_name)

            if self.remove_metric(metrics, Metric.ROW_COUNT):
                self.ensure_metric(scan_configuration.metrics, Metric.ROW_COUNT, f'{Metric.ROW_COUNT} {column_name}')

            missing = None
            if any(cfg in column_dict.keys() for cfg in COLUMN_MISSING_KEYS):
                missing = Missing()
                missing.values = column_dict.get(COLUMN_KEY_MISSING_VALUES)
                missing.format = column_dict.get(COLUMN_KEY_MISSING_FORMAT)
                missing.regex = column_dict.get(COLUMN_KEY_MISSING_REGEX)

            validity = None

            if any(cfg in column_dict.keys() for cfg in COLUMN_VALID_KEYS):
                validity = Validity()
                validity.format = column_dict.get(COLUMN_KEY_VALID_FORMAT)
                if validity.format is not None and Validity.FORMATS.get(validity.format) is None:
                    self.warning(f'Invalid {column_name}.{COLUMN_KEY_VALID_FORMAT}: {validity.format}')
                validity.regex = column_dict.get(COLUMN_KEY_VALID_REGEX)
                validity.values = column_dict.get(COLUMN_KEY_VALID_VALUES)
                validity.min = column_dict.get(COLUMN_KEY_VALID_MIN)
                validity.max = column_dict.get(COLUMN_KEY_VALID_MAX)
                validity.min_length = column_dict.get(COLUMN_KEY_VALID_MIN_LENGTH)
                validity.max_length = column_dict.get(COLUMN_KEY_VALID_MAX_LENGTH)

            tests = self.parse_tests(column_dict,
                                     COLUMN_KEY_TESTS,
                                     context_table_name=scan_configuration.table_name,
                                     context_column_name=column_name)

            self.check_invalid_keys(COLUMN_ALL_KEYS)

            column_name_lower = column_name.lower()
            scan_configuration_columns[column_name_lower] = ScanYmlColumn(
                metrics=metrics,
                missing=missing,
                validity=validity,
                tests=tests)

            self._pop_context()

        self._pop_context()

        return scan_configuration_columns
