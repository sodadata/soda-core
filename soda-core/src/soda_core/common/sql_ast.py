from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class SELECT:
    fields: SqlExpression | str | list[SqlExpression | str]


@dataclass
class FROM:
    table_name: str
    table_prefix: Optional[list[str]] = None
    alias: Optional[str] = None

    def AS(self, alias: str) -> FROM:
        self.alias = alias
        return self

    def IN(self, table_prefix: str | list[str]) -> FROM:
        self.table_prefix = table_prefix if isinstance(table_prefix, list) else [table_prefix]
        return self


@dataclass
class LEFT_INNER_JOIN(FROM):
    on_condition: Optional[SqlExpression] = None

    def ON(self, on_condition: SqlExpression) -> FROM:
        self.on_condition = on_condition
        return self


@dataclass
class WHERE:
    condition: SqlExpression


@dataclass
class SqlExpression:
    pass


@dataclass
class STAR(SqlExpression):
    pass


@dataclass
class COUNT(SqlExpression):
    expression: SqlExpression | str


@dataclass
class SUM(SqlExpression):
    expression: SqlExpression | str


@dataclass
class SqlExpressionStr(SqlExpression):
    expression_str: str


@dataclass
class CASE_WHEN(SqlExpression):
    condition: SqlExpression | str
    if_expression: SqlExpression | str
    else_expression: SqlExpression | str


@dataclass
class IS_NULL(SqlExpression):
    expression: SqlExpression | str


@dataclass
class IN(SqlExpression):
    expression: SqlExpression | str
    list_expression: list[SqlExpression]


@dataclass
class LOWER(SqlExpression):
    expression: SqlExpression | str


@dataclass
class LENGTH(SqlExpression):
    expression: SqlExpression | str


@dataclass
class COLUMN(SqlExpression):
    name: str
    table_alias: Optional[str] = None
    field_alias: Optional[str] = None

    def IN(self, table_alias: str) -> COLUMN:
        return COLUMN(name=self.name, table_alias=table_alias, field_alias=self.field_alias)

    def AS(self, field_alias: str) -> COLUMN:
        return COLUMN(name=self.name, table_alias=self.table_alias, field_alias=field_alias)


@dataclass
class FUNCTION(SqlExpression):
    name: str
    args: list[SqlExpression | str]


@dataclass
class LITERAL(SqlExpression):
    value: object


@dataclass
class Operator(SqlExpression):
    left: SqlExpression | str
    right: SqlExpression | str


@dataclass
class EQ(Operator):
    pass


@dataclass
class NEQ(Operator):
    pass


@dataclass
class GT(Operator):
    pass


@dataclass
class GTE(Operator):
    pass


@dataclass
class LT(Operator):
    pass


@dataclass
class LTE(Operator):
    pass


@dataclass
class NOT(SqlExpression):
    expression: SqlExpression | str


@dataclass
class REGEX_LIKE(SqlExpression):
    expression: SqlExpression | str
    regex_pattern: str


@dataclass
class LIKE(Operator):
    pass


@dataclass
class NOT_LIKE(Operator):
    pass


@dataclass
class AND(SqlExpression):
    clauses: SqlExpression | str | list[SqlExpression]

    def _get_clauses_as_list(self) -> list[SqlExpression]:
        if isinstance(self.clauses, list):
            return self.clauses
        else:
            return [self.clauses]


@dataclass
class OR(SqlExpression):
    clauses: SqlExpression | str | list[SqlExpression]

    def _get_clauses_as_list(self) -> list[SqlExpression]:
        if isinstance(self.clauses, list):
            return self.clauses
        else:
            return [self.clauses]
