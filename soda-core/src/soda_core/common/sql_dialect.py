from __future__ import annotations

from datetime import date, datetime
from numbers import Number
from textwrap import dedent, indent

from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.sql_ast import *


class SqlDialect:
    DEFAULT_QUOTE_CHAR = '"'

    """
    Extends DataSource with all logic to builds the SQL queries.
    Specific DataSource's can customize their SQL queries by subclassing SqlDialect,
    overriding methods of SqlDialect and returning the customized SqlDialect in DataSource._create_sql_dialect()
    """

    def quote_default(self, identifier: Optional[str]) -> Optional[str]:
        return (
            f"{self.DEFAULT_QUOTE_CHAR}{identifier}{self.DEFAULT_QUOTE_CHAR}"
            if isinstance(identifier, str) and len(identifier) > 0
            else None
        )

    def build_fully_qualified_sql_name(self, dataset_identifier: DatasetIdentifier) -> str:
        return self.qualify_dataset_name(
            dataset_prefix=dataset_identifier.prefixes, dataset_name=dataset_identifier.dataset_name
        )

    def qualify_dataset_name(self, dataset_prefix: list[str], dataset_name: str) -> str:
        """
        Creates a fully qualified table name, optionally quoting the table name
        """
        parts: list[str] = list(dataset_prefix) if dataset_prefix else []
        parts.append(dataset_name)
        parts = [self.quote_default(p) for p in parts if p]
        return ".".join(parts)

    def literal(self, o: object) -> str:
        if o is None:
            return "NULL"
        elif isinstance(o, Number):
            return self.literal_number(o)
        elif isinstance(o, str):
            return self.literal_string(o)
        elif isinstance(o, datetime):
            return self.literal_datetime(o)
        elif isinstance(o, date):
            return self.literal_date(o)
        elif isinstance(o, list) or isinstance(o, set) or isinstance(o, tuple):
            return self.literal_list(o)
        elif isinstance(o, bool):
            return self.literal_boolean(o)
        raise RuntimeError(f"Cannot convert type {type(o)} to a SQL literal: {o}")

    def supports_varchar_length(self) -> bool:
        return True

    def literal_number(self, value: Number):
        if value is None:
            return None
        return str(value)

    def literal_string(self, value: str):
        if value is None:
            return None
        return "'" + self.escape_string(value) + "'"

    def literal_list(self, l: list):
        if l is None:
            return None
        return "(" + (",".join([self.literal(e) for e in l])) + ")"

    def literal_date(self, date: date):
        date_string = date.strftime("%Y-%m-%d")
        return f"DATE '{date_string}'"

    def literal_datetime(self, datetime: datetime):
        return f"'{datetime.isoformat()}'"

    def literal_boolean(self, boolean: bool):
        return "TRUE" if boolean is True else "FALSE"

    def escape_string(self, value: str):
        # string_literal: str = re.sub(r"(\\.)", r"\\\1", value)
        string_literal: str = value.replace("'", "''")
        return string_literal

    def escape_regex(self, value: str):
        return value

    def create_schema_if_not_exists_sql(self, schema_name: str) -> str:
        quoted_schema_name: str = self.quote_default(schema_name)
        return f"CREATE SCHEMA IF NOT EXISTS {quoted_schema_name};"

    def build_select_sql(self, select_elements: list, add_semicolon: bool = True) -> str:
        statement_lines: list[str] = []
        statement_lines.extend(self._build_cte_sql_lines(select_elements))
        statement_lines.extend(self._build_select_sql_lines(select_elements))
        statement_lines.extend(self._build_from_sql_lines(select_elements))
        statement_lines.extend(self._build_where_sql_lines(select_elements))
        statement_lines.extend(self._build_order_by_lines(select_elements))
        return "\n".join(statement_lines) + (";" if add_semicolon else "")

    def _build_select_sql_lines(self, select_elements: list) -> list[str]:
        select_field_sqls: list[str] = []
        for select_element in select_elements:
            if isinstance(select_element, SELECT):
                if isinstance(select_element.fields, str) or isinstance(select_element.fields, SqlExpression):
                    select_element.fields = [select_element.fields]
                for select_field in select_element.fields:
                    if isinstance(select_field, str):
                        select_field_sqls.append(self.quote_default(select_field))
                    elif isinstance(select_field, SqlExpression):
                        select_field_sqls.append(self.build_expression_sql(select_field))
                    else:
                        raise Exception(f"Invalid select field type: {select_field.__class__.__name__}")

        # Alternatively, concatenate all the fields on one line to reduce SQL statement length
        # return "SELECT " + (", ".join(select_fields_sql))
        # For now, we opt for SELECT statement readability...

        select_sql_lines: list[str] = []
        for i in range(0, len(select_field_sqls)):
            if i == 0:
                sql_line = f"SELECT {select_field_sqls[0]}"
            else:
                sql_line = f"       {select_field_sqls[i]}"
            # Append comma all lines except the last one
            if i < len(select_field_sqls) - 1:
                sql_line += ","
            select_sql_lines.append(sql_line)

        return select_sql_lines

    def _build_cte_sql_lines(self, select_elements: list) -> list[str]:
        cte_lines: list[str] = []
        for select_element in select_elements:
            if isinstance(select_element, WITH):
                cte_query_sql_str: str | None = None
                if isinstance(select_element.cte_query, list):
                    cte_query_sql_str = self.build_select_sql(select_element.cte_query)
                    cte_query_sql_str = cte_query_sql_str.strip()
                elif isinstance(select_element.cte_query, str):
                    cte_query_sql_str = dedent(select_element.cte_query).strip()
                if cte_query_sql_str:
                    cte_query_sql_str = cte_query_sql_str.rstrip(";")
                    indented_nested_query: str = indent(cte_query_sql_str, "  ")
                    cte_lines.append(f"WITH {self.quote_default(select_element.alias)} AS (")
                    cte_lines.extend(indented_nested_query.split("\n"))
                    cte_lines.append(f")")
        return cte_lines

    def build_expression_sql(self, expression: SqlExpression | str | Number) -> str:
        if isinstance(expression, str):
            return self.quote_default(expression)
        elif isinstance(expression, Number):
            return str(expression)
        elif isinstance(expression, COLUMN):
            return self._build_column_sql(expression)
        elif isinstance(expression, LITERAL):
            return self.literal(expression.value)
        elif isinstance(expression, OR):
            return self._build_or_sql(expression)
        elif isinstance(expression, AND):
            return self._build_and_sql(expression)
        elif isinstance(expression, NOT):
            return self._build_not_sql(expression)
        elif isinstance(expression, Operator):
            return self._build_operator_sql(expression)
        elif isinstance(expression, COUNT):
            return self._build_count_sql(expression)
        elif isinstance(expression, SUM):
            return self._build_sum_sql(expression)
        elif isinstance(expression, CASE_WHEN):
            return self._build_case_when_sql(expression)
        elif isinstance(expression, TUPLE):
            return self._build_tuple_sql(expression)
        elif isinstance(expression, IS_NULL):
            return self._build_is_null_sql(expression)
        elif isinstance(expression, IS_NOT_NULL):
            return self._build_is_not_null_sql(expression)
        elif isinstance(expression, REGEX_LIKE):
            return self._build_regex_like_sql(expression)
        elif isinstance(expression, LIKE):
            return self._build_like_sql(expression)
        elif isinstance(expression, IN):
            return self._build_in_sql(expression)
        elif isinstance(expression, NOT_LIKE):
            return self._build_not_like_sql(expression)
        elif isinstance(expression, LOWER):
            return self._build_lower_sql(expression)
        elif isinstance(expression, LENGTH):
            return self._build_length_sql(expression)
        elif isinstance(expression, MAX):
            return self._build_max_sql(expression)
        elif isinstance(expression, FUNCTION):
            return self._build_function_sql(expression)
        elif isinstance(expression, DISTINCT):
            return self._build_distinct_sql(expression)
        elif isinstance(expression, SqlExpressionStr):
            return f"({expression.expression_str})"
        elif isinstance(expression, ORDINAL_POSITION):
            return self._build_ordinal_position_sql(expression)
        elif isinstance(expression, STAR):
            return "*"
        raise Exception(f"Invalid expression type {expression.__class__.__name__}")

    def _build_column_sql(self, column: COLUMN) -> str:
        table_alias_sql: str = f"{self.quote_default(column.table_alias)}." if column.table_alias else ""
        column_sql: str = self.quote_default(column.name)
        field_alias_sql: str = f" AS {self.quote_default(column.field_alias)}" if column.field_alias else ""
        return f"{table_alias_sql}{column_sql}{field_alias_sql}"

    def _build_or_sql(self, or_expr: OR) -> str:
        if isinstance(or_expr.clauses, list) and len(or_expr.clauses) == 1:
            return self.build_expression_sql(or_expr.clauses[0])
        if isinstance(or_expr.clauses, str) or isinstance(or_expr.clauses, SqlExpression):
            return self.build_expression_sql(or_expr.clauses)
        or_clauses_sql: str = " OR ".join(self.build_expression_sql(or_clause) for or_clause in or_expr.clauses)
        return f"({or_clauses_sql})"

    def _build_not_sql(self, not_expr: NOT) -> str:
        expr_sql: str = self.build_expression_sql(not_expr.expression)
        return f"NOT({expr_sql})"

    def _build_and_sql(self, and_expr: AND) -> str:
        if isinstance(and_expr.clauses, list) and len(and_expr.clauses) == 1:
            return self.build_expression_sql(and_expr.clauses[0])
        if isinstance(and_expr.clauses, str) or isinstance(and_expr.clauses, SqlExpression):
            return self.build_expression_sql(and_expr.clauses)
        return " AND ".join(self.build_expression_sql(and_clause) for and_clause in and_expr.clauses)

    def _build_from_sql_lines(self, select_elements: list) -> list[str]:
        sql_lines: list[str] = []
        # This method formats with newlines and indentation.
        # Alternatively, concatenate all the fields on one line to reduce SQL statement length
        # return "SELECT " + (", ".join(select_fields_sql))
        # For now, we opt for SELECT statement readability...

        from_elements: list[FROM] = [
            select_element for select_element in select_elements if isinstance(select_element, FROM)
        ]

        from_sql_line: str = "FROM "
        for from_element in from_elements:
            if type(from_element) == FROM:
                if from_element is not from_elements[0]:
                    sql_lines.append(f"{from_sql_line},")
                    from_sql_line = "     "
                from_sql_line += self._build_from_part(from_element)
            elif isinstance(from_element, LEFT_INNER_JOIN):
                sql_lines.append(from_sql_line)
                from_sql_line = f"     {self._build_left_inner_join_part(from_element)}"

        sql_lines.append(from_sql_line)
        return sql_lines

    def _build_from_part(self, from_part: FROM) -> str:
        # "fully".qualified"."tablename" [AS "table_alias"]

        from_parts: list[str] = [
            self._build_qualified_quoted_dataset_name(
                dataset_name=from_part.table_name, dataset_prefix=from_part.table_prefix
            )
        ]

        if isinstance(from_part.alias, str):
            from_parts.append(f"AS {self.quote_default(from_part.alias)}")

        return " ".join(from_parts)

    def _build_left_inner_join_part(self, left_inner_join: LEFT_INNER_JOIN) -> str:
        # [INNER JOIN] "fully".qualified"."tablename" [AS "table_alias"] [ON join_condition]

        from_parts: list[str] = []

        if isinstance(left_inner_join, LEFT_INNER_JOIN):
            from_parts.append("LEFT JOIN")

        from_parts.append(
            self._build_qualified_quoted_dataset_name(
                dataset_name=left_inner_join.table_name, dataset_prefix=left_inner_join.table_prefix
            )
        )

        if isinstance(left_inner_join.alias, str):
            from_parts.append(f"AS {self.quote_default(left_inner_join.alias)}")

        if isinstance(left_inner_join, LEFT_INNER_JOIN):
            from_parts.append(f"ON {self.build_expression_sql(left_inner_join.on_condition)}")

        return " ".join(from_parts)

    def _build_qualified_quoted_dataset_name(self, dataset_name: str, dataset_prefix: Optional[list[str]]) -> str:
        name_parts: list[str] = [] if dataset_prefix is None else list(dataset_prefix)
        name_parts.append(dataset_name)
        quoted_name_parts: list[str] = [self.quote_default(name_part) for name_part in name_parts]
        return ".".join(quoted_name_parts)

    def _build_operator_sql(self, operator: Operator) -> str:
        operators: dict[type, str] = {
            EQ: "=",
            NEQ: "!=",
            LT: "<",
            LTE: "<=",
            GT: ">",
            GTE: ">=",
            LIKE: "like",
        }
        operator_sql: str = operators[type(operator)]
        return f"{self.build_expression_sql(operator.left)} {operator_sql} {self.build_expression_sql(operator.right)}"

    def _build_where_sql_lines(self, select_elements: list) -> list[str]:
        and_expressions: list[SqlExpression] = []
        for select_element in select_elements:
            if isinstance(select_element, WHERE):
                and_expressions.append(select_element.condition)
            elif isinstance(select_element, AND):
                and_expressions.extend(select_element._get_clauses_as_list())

        where_parts: list[str] = [self.build_expression_sql(and_expression) for and_expression in and_expressions]

        where_sql_lines: list[str] = []
        for i in range(0, len(where_parts)):
            if i == 0:
                sql_line = f"WHERE {where_parts[0]}"
            else:
                sql_line = f"  AND {where_parts[i]}"
            where_sql_lines.append(sql_line)
        return where_sql_lines

    def _build_function_sql(self, function: FUNCTION) -> str:
        args: list[SqlExpression | str] = [function.args] if not isinstance(function.args, list) else function.args
        args_sqls: list[str] = [self.build_expression_sql(arg) for arg in args]
        if function.name in ["+", "-", "/", "*"]:
            operators: str = f" {function.name} ".join(args_sqls)
            return f"({operators})"
        else:
            args_list_sql: str = ", ".join(args_sqls)
            return f"{function.name}({args_list_sql})"

    def _build_count_sql(self, count: COUNT) -> str:
        return f"COUNT({self.build_expression_sql(count.expression)})"

    def _build_distinct_sql(self, distinct: DISTINCT) -> str:
        expressions: list[SqlExpression] = (
            distinct.expression if isinstance(distinct.expression, list) else [distinct.expression]
        )
        field_expression_str = ", ".join([self.build_expression_sql(e) for e in expressions])
        return f"DISTINCT({field_expression_str})"

    def _build_sum_sql(self, sum: SUM) -> str:
        return f"SUM({self.build_expression_sql(sum.expression)})"

    def _build_is_null_sql(self, is_null: IS_NULL) -> str:
        return f"{self.build_expression_sql(is_null.expression)} IS NULL"

    def _build_is_not_null_sql(self, is_null: IS_NOT_NULL) -> str:
        return f"{self.build_expression_sql(is_null.expression)} IS NOT NULL"

    def _build_in_sql(self, in_: IN) -> str:
        list_expressions: str = ", ".join([self.build_expression_sql(element) for element in in_.list_expression])
        return f"{self.build_expression_sql(in_.expression)} IN ({list_expressions})"

    def _build_like_sql(self, like: LIKE) -> str:
        return f"{self.build_expression_sql(like.left)} LIKE {self.build_expression_sql(like.right)}"

    def _build_not_like_sql(self, not_like: NOT_LIKE) -> str:
        return f"{self.build_expression_sql(not_like.left)} NOT LIKE {self.build_expression_sql(not_like.right)}"

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"REGEXP_LIKE({expression}, '{matches.regex_pattern}')"

    def _build_lower_sql(self, lower: LOWER) -> str:
        return f"LOWER({self.build_expression_sql(lower.expression)})"

    def _build_length_sql(self, length: LENGTH) -> str:
        return f"LENGTH({self.build_expression_sql(length.expression)})"

    def _build_max_sql(self, max: MAX) -> str:
        return f"MAX({self.build_expression_sql(max.expression)})"

    def _build_case_when_sql(self, case_when: CASE_WHEN) -> str:
        return (
            f"CASE WHEN {self.build_expression_sql(case_when.condition)} "
            + f"THEN {self.build_expression_sql(case_when.if_expression)} "
            + (f"ELSE {self.build_expression_sql(case_when.else_expression)} " if case_when.else_expression else "")
            + "END"
        )

    def _build_order_by_lines(self, select_elements: list) -> list[str]:
        order_by_clauses: list[str] = []
        for select_element in select_elements:
            if isinstance(select_element, ORDER_BY_ASC) or isinstance(select_element, ORDER_BY_DESC):
                expression = select_element.expression
                direction: str = " ASC" if isinstance(select_element, ORDER_BY_ASC) else " DESC"
                order_by_clauses.append(f"{self.build_expression_sql(expression)}{direction}")
        if order_by_clauses:
            order_by_text: str = ", ".join(order_by_clauses)
            return [f"ORDER BY {order_by_text}"]
        else:
            return []

    def _build_ordinal_position_sql(self, ordinal_position: ORDINAL_POSITION) -> str:
        return "ORDINAL_POSITION"

    def supports_function(self, function: str) -> bool:
        return function in ["avg", "avg_length", "max", "min", "max_length", "min_length", "sum"]

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        elements: str = ", ".join(self.build_expression_sql(e) for e in tuple.expressions)
        return f"({elements})"

    def schema_information_schema(self) -> str:
        """
        Name of the schema that has the metadata
        """
        return self.default_casify("information_schema")

    def table_tables(self) -> str:
        """
        Name of the table that has the table information in the metadata
        """
        return self.default_casify("tables")

    def table_columns(self) -> str:
        """
        Name of the table that has the columns information in the metadata.
        Purpose of this method is to allow specific data source to override.
        """
        return self.default_casify("columns")

    def column_table_catalog(self) -> str:
        """
        Name of the column that has the database information in the tables metadata table
        """
        return self.default_casify("table_catalog")

    def column_table_schema(self) -> str:
        """
        Name of the column that has the schema information in the tables metadata table
        """
        return self.default_casify("table_schema")

    def column_table_name(self) -> str:
        """
        Name of the column that has the table name in the tables metadata table
        """
        return self.default_casify("table_name")

    def column_column_name(self) -> str:
        """
        Name of the column that has the column name in the tables metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return self.default_casify("column_name")

    def column_data_type(self) -> str:
        """
        Name of the column that has the data type in the tables metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return self.default_casify("data_type")

    def column_data_type_max_length(self) -> str:
        """
        Name of the column that has the max data type length in the tables metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return self.default_casify("character_maximum_length")

    def default_casify(self, identifier: str) -> str:
        return identifier.lower()

    # Very lightweight dialect-specific interpretation of dataset prefixes.
    def get_database_prefix_index(self) -> int | None:
        return 0

    # Very lightweight dialect-specific interpretation of dataset prefixes.
    def get_schema_prefix_index(self) -> int | None:
        return 1

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        return f"timestamp '{datetime_in_iso8601}'"

    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        return f"date_trunc('day', {timestamp_literal})"

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        return f"{timestamp_literal} + interval '1 day'"

    def quote_column(self, column_name: str) -> str:
        return self.quote_default(column_name)
