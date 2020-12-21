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
from sodasql.scan.column import Column
from sodasql.scan.metric import Metric
from sodasql.scan.missing import Missing
from sodasql.scan.scan_configuration import ScanConfiguration
from sodasql.scan.validity import Validity
from sodasql.warehouse.dialect import Dialect


class ScanColumnCache:
    # Temporary stores computed conditions and where clauses for missing and validity

    def __init__(self, scan_configuration: ScanConfiguration, column: Column, dialect: Dialect):
        self.missing = scan_configuration.get_missing(column)
        self.is_missing_metric_enabled = scan_configuration.is_any_metric_enabled(column.name, [
            Metric.MISSING_COUNT, Metric.MISSING_PERCENTAGE,
            Metric.VALUES_COUNT, Metric.VALUES_PERCENTAGE])
        self.missing_condition = self.get_missing_condition(column, self.missing, dialect)

        self.validity = scan_configuration.get_validity(column)
        self.is_validity_metric_enabled = scan_configuration.is_any_metric_enabled(column.name, [
            Metric.INVALID_COUNT, Metric.INVALID_PERCENTAGE,
            Metric.VALID_COUNT, Metric.VALID_PERCENTAGE])
        self.valid_condition = self.get_valid_condition(column, self.validity, dialect)

        self.non_missing_and_valid_condition = \
            f'NOT {self.missing_condition} AND {self.valid_condition}' if self.valid_condition \
            else f'NOT {self.missing_condition}'

        self.is_text: bool = False
        self.is_number: bool = False
        self.is_column_numeric_text_format: bool = False
        self.validity_format: str = None

    def get_missing_condition(self, column: Column, missing: Missing, dialect: Dialect):
        quoted_column_name = dialect.qualify_column_name(column.name)
        if missing is None:
            return f'{quoted_column_name} IS NULL'
        validity_clauses = [f'{quoted_column_name} IS NULL']
        if missing.values is not None:
            sql_expr_missing_values = dialect.sql_expr_list(column, missing.values)
            validity_clauses.append(f'{quoted_column_name} IN {sql_expr_missing_values}')
        if missing.format is not None:
            format_regex = Missing.FORMATS.get(missing.format)
            validity_clauses.append(dialect.sql_expr_regexp_like(quoted_column_name, format_regex))
        if missing.regex is not None:
            validity_clauses.append(dialect.sql_expr_regexp_like(quoted_column_name, missing.regex))
        return '(' + ' OR '.join(validity_clauses) + ')'

    def get_valid_condition(self, column: Column, validity: Validity, dialect: Dialect):
        quoted_column_name = dialect.qualify_column_name(column.name)
        if validity is None:
            return None
        validity_clauses = []
        if validity.format:
            format_regex = Validity.FORMATS.get(validity.format)
            validity_clauses.append(dialect.sql_expr_regexp_like(quoted_column_name, format_regex))
        if validity.regex:
            validity_clauses.append(dialect.sql_expr_regexp_like(quoted_column_name, validity.regex))
        if validity.min_length:
            validity_clauses.append(f'{dialect.sql_expr_length(quoted_column_name)} >= {validity.min_length}')
        if validity.max_length:
            validity_clauses.append(f'{dialect.sql_expr_length(quoted_column_name)} <= {validity.max_length}')
        # TODO add min and max clauses
        return '(' + ' AND '.join(validity_clauses) + ')'
