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
from typing import List, Optional, Set

from sodasql.scan.parse_logs import ParseLogs


class Metric:

    ROW_COUNT = 'row_count'
    SCHEMA = 'schema'

    MISSING_COUNT = 'missing_count'
    MISSING_PERCENTAGE = 'missing_percentage'
    VALUES_COUNT = 'values_count'
    VALUES_PERCENTAGE = 'values_percentage'
    VALID_COUNT = 'valid_count'
    VALID_PERCENTAGE = 'valid_percentage'
    INVALID_COUNT = 'invalid_count'
    INVALID_PERCENTAGE = 'invalid_percentage'
    MIN = 'min'
    MAX = 'max'
    AVG = 'avg'
    SUM = 'sum'
    VARIANCE = 'variance'
    STDDEV = 'stddev'
    MIN_LENGTH = 'min_length'
    MAX_LENGTH = 'max_length'
    AVG_LENGTH = 'avg_length'
    DISTINCT = 'distinct'
    UNIQUE_COUNT = 'unique_count'
    DUPLICATE_COUNT = 'duplicate_count'
    UNIQUENESS = 'uniqueness'
    MAXS = 'maxs'
    MINS = 'mins'
    FREQUENT_VALUES = 'frequent_values'
    HISTOGRAM = 'histogram'

    CATEGORY_MISSING = 'missing'
    CATEGORY_MISSING_METRICS = [MISSING_COUNT, MISSING_PERCENTAGE, VALUES_COUNT, VALUES_PERCENTAGE]

    CATEGORY_VALIDITY = 'validity'
    CATEGORY_VALIDITY_METRICS = [VALID_COUNT, VALID_PERCENTAGE, INVALID_COUNT, INVALID_PERCENTAGE]

    CATEGORY_DISTINCT = 'distinct'
    CATEGORY_DISTINCT_METRICS = [DISTINCT, UNIQUE_COUNT, UNIQUENESS, DUPLICATE_COUNT]

    METRIC_TYPES = [
        ROW_COUNT,
        SCHEMA,
        MISSING_COUNT,
        MISSING_PERCENTAGE,
        VALUES_COUNT,
        VALUES_PERCENTAGE,
        VALID_COUNT,
        VALID_PERCENTAGE,
        INVALID_COUNT,
        INVALID_PERCENTAGE,
        MIN,
        MAX,
        AVG,
        SUM,
        VARIANCE,
        STDDEV,
        MIN_LENGTH,
        MAX_LENGTH,
        AVG_LENGTH,
        DISTINCT,
        UNIQUE_COUNT,
        UNIQUENESS,
        DUPLICATE_COUNT,
        MAXS,
        MINS,
        FREQUENT_VALUES,
        HISTOGRAM
    ]


def resolve_metrics(configured_metrics_list: List[str],
                    parse_logs: ParseLogs,
                    column_name: Optional[str] = None):

    if configured_metrics_list is None:
        return set()

    if not isinstance(configured_metrics_list, list):
        parse_logs.error('metrics is not a list')
        return set()

    metrics: Set[str] = set(configured_metrics_list)

    resolve_category(metrics, Metric.CATEGORY_MISSING,  Metric.CATEGORY_MISSING_METRICS, parse_logs, column_name)
    resolve_category(metrics, Metric.CATEGORY_VALIDITY, Metric.CATEGORY_VALIDITY_METRICS, parse_logs, column_name)
    resolve_category(metrics, Metric.CATEGORY_DISTINCT, Metric.CATEGORY_DISTINCT_METRICS, parse_logs, column_name)

    if Metric.VALID_COUNT in metrics:
        ensure_metric(metrics, Metric.MISSING_COUNT,      Metric.CATEGORY_VALIDITY, parse_logs, column_name)
        ensure_metric(metrics, Metric.MISSING_PERCENTAGE, Metric.CATEGORY_VALIDITY, parse_logs, column_name)
        ensure_metric(metrics, Metric.VALUES_COUNT,       Metric.CATEGORY_VALIDITY, parse_logs, column_name)
        ensure_metric(metrics, Metric.VALUES_PERCENTAGE,  Metric.CATEGORY_VALIDITY, parse_logs, column_name)

    if any(m in metrics for m in Metric.CATEGORY_MISSING_METRICS):
        ensure_metric(metrics, Metric.ROW_COUNT, Metric.CATEGORY_MISSING, parse_logs)

    if Metric.HISTOGRAM in metrics:
        ensure_metric(metrics, Metric.MIN, Metric.HISTOGRAM, parse_logs, column_name)
        ensure_metric(metrics, Metric.MAX, Metric.HISTOGRAM, parse_logs, column_name)

    parse_logs.warning_invalid_elements(
        metrics,
        Metric.METRIC_TYPES,
        'Invalid metrics value')

    return set(metrics)


def ensure_metric(metrics: Set[str],
                  metric: str,
                  dependent_metric: str,
                  parse_logs: ParseLogs,
                  column_name: str = None):
    if metric not in metrics:
        metrics.add(metric)
        column_message = f' on column {column_name}' if column_name else ''
        parse_logs.info(f'Added metric {metric} as dependency of {dependent_metric}{column_message}')


def is_metric_category_enabled(metrics: Set[str], category: str, category_metrics: List[str]):
    if category in metrics:
        return True
    for category_metric in category_metrics:
        if category_metric in metrics:
            return True
    return False


def resolve_category(metrics: Set[str],
                     category: str,
                     category_metrics: List[str],
                     parse_logs: ParseLogs,
                     column_name: str = None):
    if is_metric_category_enabled(metrics, category, category_metrics):
        if category in metrics:
            metrics.remove(category)
        for category_metric in category_metrics:
            ensure_metric(metrics, category_metric, category, parse_logs, column_name)


def remove_metric(metrics: Set[str], metric: str):
    """Returns True if the metric was removed from the set"""
    if metric in metrics:
        metrics.remove(metric)
        return True
    else:
        return False
