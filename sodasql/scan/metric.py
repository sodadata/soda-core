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
    MIN_LENGTH = 'min_length'
    MAX_LENGTH = 'max_length'
    AVG_LENGTH = 'avg_length'
    DISTINCT = 'distinct'
    UNIQUENESS = 'uniqueness'
    UNIQUE_COUNT = 'unique_count'
    MAXS = 'maxs'
    MINS = 'mins'
    FREQUENT_VALUES = 'frequent_values'

    METRIC_TYPES = [
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
        MIN_LENGTH,
        MAX_LENGTH,
        AVG_LENGTH,
        DISTINCT,
        UNIQUENESS,
        MAXS,
        MINS,
        FREQUENT_VALUES
    ]
