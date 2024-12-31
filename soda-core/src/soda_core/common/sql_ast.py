from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SELECT:
    fields: SqlExpression | str | list[SqlExpression | str]


@dataclass
class FROM:
    table_name: str
    table_prefix: list[str] | None = None
    alias: str | None = None

    def AS(self, alias: str) -> FROM:
        return FROM(table_name=self.table_name, table_prefix=self.table_prefix, alias=alias)

    def IN(self, table_prefixes: str | list[str]) -> FROM:
        table_prefixes = table_prefixes if isinstance(table_prefixes, list) else [table_prefixes]
        return FROM(table_name=self.table_name, table_prefix=table_prefixes, alias=self.alias)


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
    if_value: SqlExpression | str
    else_value: SqlExpression | str


@dataclass
class IS_NULL(SqlExpression):
    expression: SqlExpression | str


@dataclass
class LOWER(SqlExpression):
    expression: SqlExpression | str


@dataclass
class COLUMN(SqlExpression):
    name: str
    table_alias: str | None = None
    field_alias: str | None = None

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
