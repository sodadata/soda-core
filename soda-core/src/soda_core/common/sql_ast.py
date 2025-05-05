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
class WITH:
    alias: str
    cte_query: list | str | None = None

    def AS(self, cte_query: list | str) -> WITH:
        self.cte_query = cte_query
        return self


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
class DISTINCT(SqlExpression):
    expression: SqlExpression | str | list[SqlExpression | str]


@dataclass
class SUM(SqlExpression):
    expression: SqlExpression | str


@dataclass
class SqlExpressionStr(SqlExpression):
    expression_str: str

    @classmethod
    def optional(cls, expression_str: Optional[str]) -> Optional[SqlExpressionStr]:
        return SqlExpressionStr(expression_str) if expression_str is not None else None


@dataclass
class CASE_WHEN(SqlExpression):
    condition: SqlExpression | str
    if_expression: SqlExpression | str
    else_expression: SqlExpression | str | None = None


@dataclass
class TUPLE(SqlExpression):
    expressions: list[SqlExpression | str]


@dataclass
class IS_NULL(SqlExpression):
    expression: SqlExpression | str


@dataclass
class IS_NOT_NULL(SqlExpression):
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
class MAX(SqlExpression):
    expression: SqlExpression | str


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

    @classmethod
    def optional(cls, expression: SqlExpression | str | None) -> Optional[NOT]:
        return NOT(expression) if expression is not None else None


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

    @classmethod
    def optional(cls, clauses: SqlExpression | str | list[SqlExpression] | None) -> Optional[SqlExpression]:
        if isinstance(clauses, SqlExpression):
            clauses = [clauses]
        elif isinstance(clauses, str):
            clauses = [COLUMN(clauses)]
        if isinstance(clauses, list):
            clauses = [c for c in clauses if c is not None]
            if len(clauses) == 0:
                return None
            elif len(clauses) == 1:
                return clauses[0]
            return AND(clauses)


@dataclass
class OR(SqlExpression):
    clauses: SqlExpression | str | list[SqlExpression]

    def _get_clauses_as_list(self) -> list[SqlExpression]:
        if isinstance(self.clauses, list):
            return self.clauses
        else:
            return [self.clauses]

    @classmethod
    def optional(cls, clauses: SqlExpression | str | list[SqlExpression] | None) -> Optional[SqlExpression]:
        if isinstance(clauses, SqlExpression):
            clauses = [clauses]
        elif isinstance(clauses, str):
            clauses = [COLUMN(clauses)]
        if isinstance(clauses, list):
            clauses = [c for c in clauses if c is not None]
            if len(clauses) == 0:
                return None
            elif len(clauses) == 1:
                return clauses[0]
            return OR(clauses)


@dataclass
class ORDER_BY_ASC:
    """
    Multiple of these can be added to a sql select statement. They are added in order.
    """

    expression: SqlExpression | str


@dataclass
class ORDER_BY_DESC:
    """
    Multiple of these can be added to a sql select statement. They are added in order.
    """

    expression: SqlExpression | str


@dataclass
class ORDINAL_POSITION(SqlExpression):
    pass
