from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from numbers import Number


class SqlDialect:
    """
    Extends DataSource with all logic to builds the SQL queries.
    Specific DataSource's can customize their SQL queries by subclassing SqlDialect,
    overriding methods of SqlDialect and returning the customized SqlDialect in DataSource._create_sql_dialect()
    """

    def __init__(self):
        self.default_quote_char = self._get_default_quote_char()

    def _get_default_quote_char(self) -> str:
        return '"'

    def quote_default(self, identifier: str | None) -> str | None:
        return (
            f"{self.default_quote_char}{identifier}{self.default_quote_char}"
            if isinstance(identifier, str) and len(identifier) > 0
            else None
        )

    def default_casify(self, identifier: str) -> str:
        return identifier.lower()

    def qualify_table(
        self, database_name: str | None, schema_name: str | None, table_name: str
    ) -> str:
        """
        Creates a fully qualified table name, optionally quoting the table name
        """
        parts = [self.quote_default(database_name), self.quote_default(schema_name), self.quote_default(table_name)]
        return ".".join([p for p in parts if p])

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
        return re.sub(r"(\\.)", r"\\\1", value)

    def escape_regex(self, value: str):
        return value

    def build_select_sql(self, select_elements: list) -> str:
        statement_lines: list[str] = self._build_select_sql_lines(select_elements)
        statement_lines.extend(self._build_from_sql_lines(select_elements))
        statement_lines.extend(self._build_where_sql_lines(select_elements))
        return "\n".join(statement_lines) + ";"

    def _build_select_sql_lines(self, select_elements: list) -> list[str]:
        select_field_sqls: list[str] = []
        for select_element in select_elements:
            if isinstance(select_element, SELECT):
                if isinstance(select_element.fields, str) or isinstance(select_element.fields, Expression):
                    select_element.fields = [select_element.fields]
                for select_field in select_element.fields:
                    if isinstance(select_field, str):
                        select_field_sqls.append(self.quote_default(select_field))
                    elif isinstance(select_field, Expression):
                        select_field_sqls.append(self._build_expression_sql(select_field))
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

    def _build_expression_sql(self, expression: Expression | str) -> str:
        if isinstance(expression, str):
            return self.quote_default(expression)
        elif isinstance(expression, COLUMN):
            return self._build_column_sql(expression)
        elif isinstance(expression, LITERAL):
            return self.literal(expression.value)
        elif isinstance(expression, OR):
            return self._build_or_sql(expression)
        elif isinstance(expression, AND):
            return self._build_and_sql(expression)
        elif isinstance(expression, EQ):
            return self._build_eq_sql(expression)
        elif isinstance(expression, STAR):
            return "*"
        raise Exception(f"Invalid expression type {expression.__class__.__name__}")

    def _build_column_sql(self, column: COLUMN) -> str:
        alias_sql: str = f"{self.quote_default(column.table_alias)}." if column.table_alias else ""
        column_sql: str = self.quote_default(column.name)
        return f"{alias_sql}{column_sql}"

    def _build_or_sql(self, or_expr: OR) -> str:
        if isinstance(or_expr.clauses, str) or isinstance(or_expr.clauses, Expression):
            return self._build_expression_sql(or_expr.clauses)
        or_clauses_sql: str = " OR ".join(
            self._build_expression_sql(or_clause)
            for or_clause in or_expr.clauses
        )
        return f"({or_clauses_sql})"

    def _build_and_sql(self, and_expr: AND) -> str:
        if isinstance(and_expr.clauses, str) or isinstance(and_expr.clauses, Expression):
            return self._build_expression_sql(and_expr.clauses)
        return " AND ".join(
            self._build_expression_sql(and_clause)
            for and_clause in and_expr.clauses
        )

    def _build_from_sql_lines(self, select_elements: list) -> list[str]:
        from_parts: list[str] = []
        for select_element in select_elements:
            if isinstance(select_element, FROM):
                from_parts.append(self._build_from_part(select_element))

        # Alternatively, concatenate all the fields on one line to reduce SQL statement length
        # return "SELECT " + (", ".join(select_fields_sql))
        # For now, we opt for SELECT statement readability...

        from_sql_lines: list[str] = []
        for i in range(0, len(from_parts)):
            if i == 0:
                sql_line = f"FROM {from_parts[0]}"
            else:
                sql_line = f"     {from_parts[i]}"
            # Append comma all lines except the last one
            if i < len(from_parts) - 1:
                sql_line += ", "
            from_sql_lines.append(sql_line)

        return from_sql_lines

    def _build_from_part(self, from_clause: FROM) -> str:
        table_parts_quoted: list[str] = []
        if from_clause.table_prefixes:
            table_parts_quoted.extend([
                self.quote_default(prefix_part)
                for prefix_part in from_clause.table_prefixes
            ])
        table_parts_quoted.append(self.quote_default(from_clause.table_name))
        from_part: str = ".".join(table_parts_quoted)
        if from_clause.alias:
            from_part += f" AS {self.quote_default(from_clause.alias)}"
        return from_part

    def _build_eq_sql(self, eq: EQ) -> str:
        return f"{self._build_expression_sql(eq.left)} = {self._build_expression_sql(eq.right)}"

    def _build_where_sql_lines(self, select_elements: list) -> list[str]:
        and_expressions: list[Expression] = []
        for select_element in select_elements:
            if isinstance(select_element, WHERE):
                and_expressions.append(select_element.condition)
            elif isinstance(select_element, AND):
                and_expressions.extend(select_element._get_clauses_as_list())

        where_parts: list[str] = [
            self._build_expression_sql(and_expression)
            for and_expression in and_expressions
        ]

        where_sql_lines: list[str] = []
        for i in range(0, len(where_parts)):
            if i == 0:
                sql_line = f"WHERE {where_parts[0]}"
            else:
                sql_line = f"  AND {where_parts[i]}"
            where_sql_lines.append(sql_line)
        return where_sql_lines


@dataclass
class SELECT:
    fields: Expression | str | list[Expression | str]


@dataclass
class FROM:
    table_name: str
    table_prefixes: list[str] | None = None
    alias: str | None = None

    def AS(self, alias: str) -> FROM:
        return FROM(table_name=self.table_name, table_prefixes=self.table_prefixes, alias=alias)

    def IN(self, table_prefixes: str | list[str]) -> FROM:
        table_prefixes = table_prefixes if isinstance(table_prefixes, list) else [table_prefixes]
        return FROM(table_name=self.table_name, table_prefixes=table_prefixes, alias=self.alias)


@dataclass
class WHERE:
    condition: Expression


@dataclass
class Expression:
    pass


@dataclass
class STAR(Expression):
    pass


@dataclass
class COLUMN(Expression):
    name: str
    table_alias: str | None = None

    def IN(self, table_alias: str) -> COLUMN:
        return COLUMN(name=self.name, table_alias=table_alias)


@dataclass
class LITERAL(Expression):
    value: object


@dataclass
class EQ(Expression):
    left: Expression | str
    right: Expression | str


@dataclass
class AND(Expression):
    clauses: Expression | str | list[Expression]

    def _get_clauses_as_list(self) -> list[Expression]:
        if isinstance(self.clauses, list):
            return self.clauses
        else:
            return [self.clauses]


@dataclass
class OR(Expression):
    clauses: Expression | str | list[Expression]

    def _get_clauses_as_list(self) -> list[Expression]:
        if isinstance(self.clauses, list):
            return self.clauses
        else:
            return [self.clauses]
