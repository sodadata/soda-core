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


KEY_METRICS = 'metrics'
KEY_TESTS = 'tests'

KEY_MISSING_VALUES = 'missing_values'
KEY_MISSING_FORMAT = 'missing_format'
KEY_MISSING_REGEX = 'missing_regex'

KEY_VALID_FORMAT = 'valid_format'
KEY_VALID_REGEX = 'valid_regex'
KEY_VALID_VALUES = 'valid_values'
KEY_VALID_MIN = 'valid_min'
KEY_VALID_MAX = 'valid_max'
KEY_VALID_MIN_LENGTH = 'valid_min_length'
KEY_VALID_MAX_LENGTH = 'valid_max_length'


class ScanColumnConfiguration:

    MISSING_KEYS = [
        KEY_MISSING_VALUES,
        KEY_MISSING_FORMAT,
        KEY_MISSING_REGEX]

    VALID_KEYS = [
        KEY_VALID_FORMAT,
        KEY_VALID_REGEX,
        KEY_VALID_VALUES,
        KEY_VALID_MIN,
        KEY_VALID_MAX,
        KEY_VALID_MIN_LENGTH,
        KEY_VALID_MAX_LENGTH]

    ALL_KEYS = MISSING_KEYS + VALID_KEYS + [
        KEY_METRICS,
        KEY_TESTS]

    @classmethod
    def resolve_metrics(cls, metrics: List[str]):
        resolved_metrics = metrics
        if 'missing' in metrics:
            resolved_metrics = [metric for metric in metrics if metric != 'missing']
            resolved_metrics.append(Metric.MISSING_COUNT)
        if 'valid' in metrics or 'invalid' in metrics:
            resolved_metrics = [metric for metric in metrics if metric != 'valid' and metric != 'invalid']
            resolved_metrics.append(Metric.VALID_COUNT)
        return resolved_metrics

    def __init__(self, column_name: str, column_dict: dict, parse_logs: ParseLogs):
        self.metrics = self.resolve_metrics(column_dict.get(KEY_METRICS, []))

        self.missing = None
        if any(cfg in column_dict.keys() for cfg in self.MISSING_KEYS):
            self.missing = Missing()
            self.missing.values = column_dict.get(KEY_MISSING_VALUES)
            self.missing.format = column_dict.get(KEY_MISSING_FORMAT)
            self.missing.regex = column_dict.get(KEY_MISSING_REGEX)

        self.validity = None

        if any(cfg in column_dict.keys() for cfg in self.VALID_KEYS):
            self.validity = Validity()
            self.validity.format = column_dict.get(KEY_VALID_FORMAT)
            if self.validity.format is not None and Validity.FORMATS.get(self.validity.format) is None:
                parse_logs.warning(f'Invalid {column_name}.{KEY_VALID_FORMAT}: {self.validity.format}')
            self.validity.regex = column_dict.get(KEY_VALID_REGEX)
            self.validity.values = column_dict.get(KEY_VALID_VALUES)
            self.validity.min = column_dict.get(KEY_VALID_MIN)
            self.validity.max = column_dict.get(KEY_VALID_MAX)
            self.validity.min_length = column_dict.get(KEY_VALID_MIN_LENGTH)
            self.validity.max_length = column_dict.get(KEY_VALID_MAX_LENGTH)

        self.tests = column_dict.get(KEY_TESTS)

        parse_logs.warning_invalid_elements(
            self.metrics,
            Metric.METRIC_TYPES,
            f'Invalid columns.{column_name} metric')

        parse_logs.warning_invalid_elements(
            column_dict.keys(),
            self.ALL_KEYS,
            f'Invalid key in columns.{column_name}')
