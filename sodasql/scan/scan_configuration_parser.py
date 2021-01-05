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
from typing import Optional, Set, List

from jinja2 import Template

from sodasql.scan.metric import Metric
from sodasql.scan.missing import Missing
from sodasql.scan.parser import Parser
from sodasql.scan.scan_configuration import ScanConfiguration
from sodasql.scan.scan_configuration_column import ScanConfigurationColumn
from sodasql.scan.validity import Validity

KEY_TABLE_NAME = 'table_name'
KEY_METRICS = 'metrics'
KEY_COLUMNS = 'columns'
KEY_MINS_MAXS_LIMIT = 'mins_maxs_limit'
KEY_FREQUENT_VALUES_LIMIT = 'frequent_values_limit'
KEY_SAMPLE_PERCENTAGE = 'sample_percentage'
KEY_SAMPLE_METHOD = 'sample_method'
KEY_TIME_FILTER = 'time_filter'

VALID_KEYS = [KEY_TABLE_NAME, KEY_METRICS, KEY_COLUMNS,
              KEY_MINS_MAXS_LIMIT, KEY_FREQUENT_VALUES_LIMIT,
              KEY_SAMPLE_PERCENTAGE, KEY_SAMPLE_METHOD, KEY_TIME_FILTER]

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


class ScanConfigurationParser(Parser):

    def __init__(self,
                 soda_project_dir: Optional[str] = None,
                 table: Optional[str] = None,
                 scan_yaml_path: str = None,
                 scan_yaml_str: Optional[str] = None,
                 scan_dict: Optional[dict] = None):
        super().__init__('['+(scan_yaml_path if scan_yaml_path else 'scan.yml')+']')

        if scan_dict is None:
            if scan_yaml_str is None:
                if scan_yaml_path is None:
                    if isinstance(soda_project_dir, str) and isinstance(table, str):
                        scan_yaml_path = os.path.join(soda_project_dir, table, 'scan.yml')
                    else:
                        self.error('No scan configured')

                if isinstance(scan_yaml_path, str):
                    scan_yaml_str = self._read_file_as_string(scan_yaml_path)
                else:
                    self.error(f"scan_yml_path is not a str {str(scan_yaml_path)}")

            if isinstance(scan_yaml_str, str):
                scan_dict = self._parse_yaml_str(scan_yaml_str)
            else:
                self.error(f"scan_yml_str is not a str {str(scan_yaml_str)}")

        self.scan_configuration = ScanConfiguration()

        self._push_context(scan_dict, self.description)

        self.scan_configuration.table_name = self.get_str_required(KEY_TABLE_NAME)
        self.scan_configuration.metrics = self.parse_metrics()
        self.scan_configuration.columns = self.parse_columns(self.scan_configuration)
        self.scan_configuration.sample_percentage = self.get_float_optional(KEY_SAMPLE_PERCENTAGE)
        self.scan_configuration.sample_method = self.get_str_optional(KEY_SAMPLE_METHOD, 'SYSTEM').upper()
        self.scan_configuration.mins_maxs_limit = self.get_int_optional(KEY_MINS_MAXS_LIMIT, 20)
        self.scan_configuration.frequent_values_limit = self.get_int_optional(KEY_FREQUENT_VALUES_LIMIT, 20)

        time_filter = self.get_dict_optional(KEY_TIME_FILTER)
        if time_filter:
            try:
                self.scan_configuration.time_filter = time_filter
                self.scan_configuration.time_filter_template = Template(time_filter)
            except Exception as e:
                self.error(f"Couldn't parse time_filter '{time_filter}': {str(e)}")

        self.check_invalid_keys(VALID_KEYS)

    def parse_metrics(self,
                      column_name: Optional[str] = None):
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

    def parse_columns(self, scan_configuration: ScanConfiguration) -> dict:
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

            tests = column_dict.get(COLUMN_KEY_TESTS)
            # TODO check the tests syntax

            self.check_invalid_keys(COLUMN_ALL_KEYS)

            column_name_lower = column_name.lower()
            scan_configuration_columns[column_name_lower] = ScanConfigurationColumn(
                metrics=metrics,
                missing=missing,
                validity=validity,
                tests=tests)

            self._pop_context()

        self._pop_context()

        return scan_configuration_columns
