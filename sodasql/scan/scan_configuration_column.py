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
from typing import List

from sodasql.scan.metric import Metric
from sodasql.scan.missing import Missing
from sodasql.scan.parse_logs import ParseLogs
from sodasql.scan.validity import Validity


class ScanConfigurationColumn:

    MISSING_KEYS = [
        'missing_values',
        'missing_format',
        'missing_regex']

    VALID_KEYS = [
        'valid_format',
        'valid_regex',
        'valid_values',
        'valid_min',
        'valid_max',
        'valid_min_length',
        'valid_max_length']

    ALL_KEYS = MISSING_KEYS + VALID_KEYS + [
        'metrics',
        'tests']

    @classmethod
    def resolve_metrics(cls, metrics: List[str]):
        resolved_metrics = metrics
        if 'missing' in metrics:
            resolved_metrics = [metric for metric in metrics if metric != 'missing']
            resolved_metrics.append('missing_count')
        if 'valid' in metrics or 'invalid' in metrics:
            resolved_metrics = [metric for metric in metrics if metric != 'valid' and metric != 'invalid']
            resolved_metrics.append('valid_count')
        return resolved_metrics

    def __init__(self, column_name: str, column_dict: dict, parse_logs: ParseLogs):
        self.metrics = self.resolve_metrics(column_dict.get('metrics', []))

        self.missing = None
        if any(cfg in column_dict.keys() for cfg in self.MISSING_KEYS):
            self.missing = Missing()
            self.missing.values = column_dict.get('missing_values')
            self.missing.format = column_dict.get('missing_format')
            self.missing.regex = column_dict.get('missing_regex')

        self.validity = None

        if any(cfg in column_dict.keys() for cfg in self.VALID_KEYS):
            self.validity = Validity()
            self.validity.format = column_dict.get('valid_format')
            if self.validity.format is not None and Validity.FORMATS.get(self.validity.format) is None:
                parse_logs.warning(f'Invalid {column_name}.valid_format: {self.validity.format}')
            self.validity.regex = column_dict.get('valid_regex')
            self.validity.values = column_dict.get('valid_values')
            self.validity.min = column_dict.get('valid_min')
            self.validity.max = column_dict.get('valid_max')
            self.validity.min_length = column_dict.get('valid_min_length')
            self.validity.max_length = column_dict.get('valid_max_length')

        self.tests = column_dict.get('tests')

        parse_logs.warning_invalid_elements(
            self.metrics,
            Metric.METRIC_TYPES,
            f'Invalid columns.{column_name} metric')

        parse_logs.warning_invalid_elements(
            column_dict.keys(),
            self.ALL_KEYS,
            f'Invalid key in columns.{column_name}')