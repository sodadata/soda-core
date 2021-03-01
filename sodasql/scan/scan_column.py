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
        self.scan_yml_column: ScanYmlColumn = \
            self.scan_yml.get_scan_yaml_column(self.column_name)

        dialect = self.scan.dialect
        self.qualified_column_name = dialect.qualify_column_name(self.column_name)
        self.is_text: bool = dialect.is_text(column_metadata.type)
        self.is_number: bool = dialect.is_number(column_metadata.type)
        self.is_time: bool = dialect.is_time(column_metadata.type)

        self.missing = self.scan_yml.get_missing(self.column_name)
        self.is_missing_metric_enabled = self.scan_yml.is_any_metric_enabled(
            [Metric.MISSING_COUNT, Metric.MISSING_PERCENTAGE,
             Metric.VALUES_COUNT, Metric.VALUES_PERCENTAGE],
            self.column_name)

        self.validity = self.scan_yml.get_validity(self.column_name)
        self.is_validity_metric_enabled = self.scan_yml.is_any_metric_enabled(
            [Metric.INVALID_COUNT, Metric.INVALID_PERCENTAGE,
             Metric.VALID_COUNT, Metric.VALID_PERCENTAGE],
            self.column_name)

        self.missing_condition, self.is_default_missing_condition = \
            self.__get_missing_condition(column_metadata, self.missing, dialect)
        self.non_missing_condition: Optional[str] = f'NOT ({self.missing_condition})'
        self.valid_condition, self.is_default_valid_condition = \
            self.__get_valid_condition(column_metadata, self.validity, dialect)
        self.non_missing_and_valid_condition: Optional[str] = \
            self.__get_non_missing_and_valid_condition(self.non_missing_condition, self.valid_condition)
        self.is_default_non_missing_and_valid_condition = \
            self.is_default_missing_condition and self.is_default_valid_condition

        self.validity_format = self.scan_yml.get_validity_format(column_metadata)
        self.is_valid_enabled = \
            (self.validity is not None and self.is_validity_metric_enabled) \
            or self.scan_yml.is_any_metric_enabled([Metric.DISTINCT, Metric.UNIQUENESS], self.column_name)

        self.is_missing_enabled = self.is_valid_enabled or self.is_missing_metric_enabled

        self.is_column_numeric_text_format = \
            isinstance(self.validity_format, str) \
            and self.validity_format.startswith('number_')

        self.numeric_expr = None
        self.mins_maxs_order_by_expr = dialect.qualify_column_name('value')

        if self.is_number:
            if self.is_default_non_missing_and_valid_condition:
                self.numeric_expr = self.qualified_column_name
            else:
                self.numeric_expr = dialect.sql_expr_conditional(self.non_missing_and_valid_condition,
                                                                 self.qualified_column_name)

        elif self.is_column_numeric_text_format:
            if self.is_default_non_missing_and_valid_condition:
                self.numeric_expr = dialect.sql_expr_cast_text_to_number(self.qualified_column_name, self.validity_format)
            else:
                self.numeric_expr = dialect.sql_expr_conditional(self.non_missing_and_valid_condition,
                                                                 dialect.sql_expr_cast_text_to_number(
                                                                     self.qualified_column_name, self.validity_format))

        self.has_numeric_values = self.is_number or self.is_column_numeric_text_format
        self.mins_maxs_limit = self.scan_yml.get_mins_maxs_limit(self.column_name)

    def is_any_metric_enabled(self, metrics: List[str]):
        for metric in metrics:
            if self.is_metric_enabled(metric):
                return True
        return False

    def is_metric_enabled(self, metric: str):
        if self.scan_yml_column \
                and self.scan_yml_column \
                and metric in self.scan_yml_column.metrics:
            return True
        if self.scan_yml \
                and self.scan_yml.metrics \
                and metric in self.scan_yml.metrics:
            return True

    @classmethod
    def __get_missing_condition(cls, column_metadata: ColumnMetadata, missing: Missing, dialect: Dialect):
        qualified_column_name = dialect.qualify_column_name(column_metadata.name)
        validity_clauses = [f'{qualified_column_name} IS NULL']
        if missing:
            if missing.values:
                sql_expr_missing_values = dialect.sql_expr_list(column_metadata, missing.values)
                validity_clauses.append(f'{qualified_column_name} IN {sql_expr_missing_values}')
            if missing.format:
                format_regex = Missing.FORMATS.get(missing.format)
                qualified_regex = dialect.qualify_regex(format_regex)
                validity_clauses.append(dialect.sql_expr_regexp_like(qualified_column_name, qualified_regex))
            if missing.regex:
                qualified_regex = dialect.qualify_regex(missing.regex)
                validity_clauses.append(dialect.sql_expr_regexp_like(qualified_column_name, qualified_regex))
        return " OR ".join(validity_clauses), len(validity_clauses) == 1

    @classmethod
    def __get_valid_condition(cls, column_metadata: ColumnMetadata, validity: Validity, dialect: Dialect):
        qualified_column_name = dialect.qualify_column_name(column_metadata.name)
        if validity is None:
            return '', True
        validity_clauses = []
        if validity.format is not None:
            format_regex = Validity.FORMATS.get(validity.format)
            qualified_regex = dialect.qualify_regex(format_regex)
            validity_clauses.append(dialect.sql_expr_regexp_like(qualified_column_name, qualified_regex))
        if validity.regex is not None:
            qualified_regex = dialect.qualify_regex(validity.regex)
            validity_clauses.append(dialect.sql_expr_regexp_like(qualified_column_name, qualified_regex))
        if validity.values is not None:
            valid_values_sql = dialect.literal_list(validity.values)
            validity_clauses.append(dialect.sql_expr_in(qualified_column_name, valid_values_sql))
        if validity.min_length is not None:
            validity_clauses.append(f'{dialect.sql_expr_length(qualified_column_name)} >= {validity.min_length}')
        if validity.max_length is not None:
            validity_clauses.append(f'{dialect.sql_expr_length(qualified_column_name)} <= {validity.max_length}')
        if validity.min is not None:
            validity_clauses.append(f'{qualified_column_name} >= {validity.min}')
        if validity.max is not None:
            validity_clauses.append(f'{qualified_column_name} <= {validity.max}')
        return '(' + ' AND '.join(validity_clauses) + ')', len(validity_clauses) == 0

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

        if self.scan.filter_sql:
            where = f'{where}\n  AND {self.scan.filter_sql}'

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
        if self.is_number or self.is_time:
            return 'value'
        if self.is_column_numeric_text_format:
            return self.scan.dialect.sql_expr_cast_text_to_number('value', self.validity_format)

    def get_order_by_cte_value_expression(self, numeric_value_expr: str):
        if self.is_number or self.is_time:
            return 'value'
        if self.is_column_numeric_text_format:
            return self.scan.dialect.sql_expr_cast_text_to_number('value', self.validity_format)
        elif self.is_text:
            return 'value'
        return None

    def get_tests(self):
        return self.scan_yml_column.tests if self.scan_yml_column and self.scan_yml_column.tests else []

    def get_histogram_buckets(self) -> int:
        # TODO make configurable
        return 20

    def get_metric_value(self, metric: str):
        return self.scan.scan_result.get(metric, self.column_name)
