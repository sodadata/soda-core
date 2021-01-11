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
from typing import List, Optional

from sodasql.scan.column_metadata import ColumnMetadata
from sodasql.scan.dialect import Dialect
from sodasql.scan.metric import Metric
from sodasql.scan.missing import Missing
from sodasql.scan.scan_yml import ScanYml
from sodasql.scan.scan_yml_column import ScanYmlColumn
from sodasql.scan.validity import Validity


class ScanColumn:
    """
    Contains column information used during the scan.
    Fields are precomputed based on the scan configuration.
    """

    def __init__(self, scan, column_metadata: ColumnMetadata):
        from sodasql.scan.scan import Scan
        self.scan: Scan = scan
        self.scan_yml: ScanYml = scan.scan_yml
        self.column = column_metadata
        self.column_name = column_metadata.name
        self.column_configuration: ScanYmlColumn = \
            self.scan_yml.get_column_configuration(self.column_name)

        dialect = self.scan.dialect
        self.qualified_column_name = dialect.qualify_column_name(self.column_name)
        self.is_text: bool = dialect.is_text(column_metadata)
        self.is_number: bool = dialect.is_number(column_metadata)

        self.missing = self.scan_yml.get_missing(self.column_name)
        self.is_missing_metric_enabled = self.scan_yml.is_any_metric_enabled(
            [Metric.MISSING_COUNT, Metric.MISSING_PERCENTAGE,
             Metric.VALUES_COUNT, Metric.VALUES_PERCENTAGE],
            self.column_name)
        self.non_missing_condition: Optional[str] = self.__get_non_missing_condition(column_metadata, self.missing, dialect)

        self.validity = self.scan_yml.get_validity(self.column_name)
        self.is_validity_metric_enabled = self.scan_yml.is_any_metric_enabled(
            [Metric.INVALID_COUNT, Metric.INVALID_PERCENTAGE,
             Metric.VALID_COUNT, Metric.VALID_PERCENTAGE],
            self.column_name)
        self.valid_condition: Optional[str] = self.__get_valid_condition(column_metadata, self.validity, dialect)

        self.non_missing_and_valid_condition: Optional[str] = \
            self.__get_non_missing_and_valid_condition(self.non_missing_condition, self.valid_condition)

        self.validity_format = self.scan_yml.get_validity_format(column_metadata)
        self.is_valid_enabled = \
            (self.validity is not None and self.is_validity_metric_enabled) \
            or self.scan_yml.is_any_metric_enabled([Metric.DISTINCT, Metric.UNIQUENESS], self.column_name)

        self.is_missing_enabled = self.is_valid_enabled or self.is_missing_metric_enabled

        self.is_column_numeric_text_format = \
            isinstance(self.validity_format, str) \
            and self.validity_format.startswith('number_')

        if self.is_number:
            self.numeric_expr = self.qualified_column_name
            self.numeric_text_expr = None
        elif self.is_column_numeric_text_format:
            self.numeric_text_expr = dialect.sql_expr_conditional(
                    self.non_missing_and_valid_condition,
                    dialect.sql_expr_cast_text_to_number(self.qualified_column_name, self.validity_format))
            self.numeric_expr = self.numeric_text_expr
        else:
            self.numeric_expr = None
            self.numeric_text_expr = None

        self.has_numeric_values = self.is_number or self.is_column_numeric_text_format
        self.mins_maxs_limit = self.scan_yml.get_mins_maxs_limit(self.column_name)

    def is_any_metric_enabled(self, metrics: List[str]):
        for metric in metrics:
            if self.is_metric_enabled(metric):
                return True
        return False

    def is_metric_enabled(self, metric: str):
        if self.column_configuration \
                and self.column_configuration \
                and metric in self.column_configuration.metrics:
            return True
        if self.scan_yml \
                and self.scan_yml.metrics \
                and metric in self.scan_yml.metrics:
            return True

    @classmethod
    def __get_non_missing_condition(cls, column_metadata: ColumnMetadata, missing: Missing, dialect: Dialect):
        if missing is None:
            return ''
        qualified_column_name = dialect.qualify_column_name(column_metadata.name)
        validity_clauses = [f'{qualified_column_name} IS NOT NULL']
        if missing.values is not None:
            sql_expr_missing_values = dialect.sql_expr_list(column_metadata, missing.values)
            validity_clauses.append(f'{qualified_column_name} NOT IN {sql_expr_missing_values}')
        if missing.format is not None:
            format_regex = Missing.FORMATS.get(missing.format)
            qualified_regex = dialect.qualify_regex(format_regex)
            validity_clauses.append(f'NOT {dialect.sql_expr_regexp_like(qualified_column_name, qualified_regex)}')
        if missing.regex is not None:
            qualified_regex = dialect.qualify_regex(missing.regex)
            validity_clauses.append(f'NOT {dialect.sql_expr_regexp_like(qualified_column_name, qualified_regex)}')
        return ' AND '.join(validity_clauses)

    @classmethod
    def __get_valid_condition(cls, column_metadata: ColumnMetadata, validity: Validity, dialect: Dialect):
        qualified_column_name = dialect.qualify_column_name(column_metadata.name)
        if validity is None:
            return ''
        validity_clauses = []
        if validity.format:
            format_regex = Validity.FORMATS.get(validity.format)
            qualified_regex = dialect.qualify_regex(format_regex)
            validity_clauses.append(dialect.sql_expr_regexp_like(qualified_column_name, qualified_regex))
        if validity.regex:
            qualified_regex = dialect.qualify_regex(validity.regex)
            validity_clauses.append(dialect.sql_expr_regexp_like(qualified_column_name, qualified_regex))
        if validity.min_length:
            validity_clauses.append(f'{dialect.sql_expr_length(qualified_column_name)} >= {validity.min_length}')
        if validity.max_length:
            validity_clauses.append(f'{dialect.sql_expr_length(qualified_column_name)} <= {validity.max_length}')
        if validity.min:
            validity_clauses.append(f'{qualified_column_name} >= {validity.min}')
        if validity.max:
            validity_clauses.append(f'{qualified_column_name} <= {validity.max}')
        return '(' + ' AND '.join(validity_clauses) + ')'

    def __get_non_missing_and_valid_condition(self, non_missing_condition, valid_condition):
        if non_missing_condition and valid_condition:
            return f'{non_missing_condition} AND {valid_condition}'
        elif non_missing_condition:
            return non_missing_condition
        elif valid_condition:
            return valid_condition
        return ''

    def get_group_by_cte(self):
        if self.non_missing_and_valid_condition and self.scan.table_sample_clause:
            where = f'{self.non_missing_and_valid_condition}\n    {self.scan.table_sample_clause}'
        elif self.non_missing_and_valid_condition:
            where = self.non_missing_and_valid_condition
        elif self.scan.table_sample_clause:
            where = self.scan.table_sample_clause
        else:
            where = f'{self.qualified_column_name} IS NOT NULL'

        if self.scan.time_filter_sql:
            where = f'{where}\n  AND {self.scan.time_filter_sql}'

        return (
            f"WITH group_by_value AS ( \n"
            f"  SELECT \n"
            f"    {self.qualified_column_name} AS value, \n"
            f"    COUNT(*) AS frequency \n"
            f"  FROM {self.scan.qualified_table_name} \n"
            f"  WHERE {where} \n"
            f"  GROUP BY {self.qualified_column_name} \n"
            f")"
        )

    def get_group_by_cte_numeric_value_expression(self):
        if self.is_number:
            return 'value'
        if self.is_column_numeric_text_format:
            return self.scan.dialect.sql_expr_cast_text_to_number('value', self.validity_format)

    def get_tests(self):
        return self.column_configuration.tests if self.column_configuration and self.column_configuration.tests else []

    def get_histogram_buckets(self) -> int:
        # TODO make configurable
        return 20

    def get_metric_value(self, metric: str):
        return self.scan.scan_result.get(metric, self.column_name)
