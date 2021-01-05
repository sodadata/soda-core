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

    CATEGORY_DUPLICATES = 'duplicates'
    CATEGORY_DUPLICATES_METRICS = [DISTINCT, UNIQUE_COUNT, UNIQUENESS, DUPLICATE_COUNT]

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
