from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


class BaseSqlExpression:
    def __init__(self):
        self.parent_node: Optional[BaseSqlExpression] = None

    def get_parent_node(self) -> Optional[BaseSqlExpression]:
        return self.parent_node

    def get_parent_nodes(self) -> list[BaseSqlExpression]:
        parent_node = self.get_parent_node()
        if parent_node is None:
            return []
        return [parent_node] + parent_node.get_parent_nodes()

    def set_parent_node(self, parent_node: BaseSqlExpression):
        self.parent_node = parent_node

    def handle_parent_node_update(self, expression: SqlExpression | str | list[SqlExpression | str] | None):
        if expression is None:
            return
        if isinstance(expression, list):
            for expression in expression:
                if isinstance(expression, BaseSqlExpression):
                    expression.set_parent_node(self)
        elif isinstance(expression, BaseSqlExpression):
            expression.set_parent_node(self)

    def check_context(self, context_node: type[BaseSqlExpression]) -> bool:
        parent_nodes = self.get_parent_nodes()
        return any(type(node) == context_node for node in parent_nodes)


@dataclass
class SELECT(BaseSqlExpression):
    fields: SqlExpression | str | list[SqlExpression | str]

    def __init__(self, fields: SqlExpression | str | list[SqlExpression | str]):
        super().__init__()
        self.fields = fields
        self.handle_parent_node_update(fields)


@dataclass
class FROM(BaseSqlExpression):
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
class LEFT_INNER_JOIN(FROM, BaseSqlExpression):
    on_condition: Optional[SqlExpression] = None

    def __init__(
        self,
        table_name: str,
        table_prefix: Optional[list[str]] = None,
        alias: Optional[str] = None,
        on_condition: Optional[SqlExpression] = None,
    ):
        super().__init__(table_name, table_prefix, alias)
        if on_condition is not None:
            self.ON(on_condition)

    def ON(self, on_condition: SqlExpression) -> FROM:
        self.on_condition = on_condition
        self.handle_parent_node_update(on_condition)
        return self


@dataclass
class WHERE(BaseSqlExpression):
    condition: SqlExpression

    def __init__(self, condition: SqlExpression):
        super().__init__()
        self.condition = condition
        self.handle_parent_node_update(condition)


@dataclass
class WITH(BaseSqlExpression):
    alias: str
    cte_query: list | str | None = None

    def __init__(self, alias: str):
        super().__init__()
        self.alias = alias

    def AS(self, cte_query: list | str) -> WITH:
        self.cte_query = cte_query
        self.handle_parent_node_update(cte_query)
        return self


@dataclass
class SqlExpression(BaseSqlExpression):
    def __init__(self):
        super().__init__()


@dataclass
class STAR(SqlExpression):
    def __init__(self):
        super().__init__()


@dataclass
class COUNT(SqlExpression):
    expression: SqlExpression | str

    def __init__(self, expression: SqlExpression | str):
        super().__init__()
        self.expression = expression
        self.handle_parent_node_update(expression)


@dataclass
class DISTINCT(SqlExpression):
    expression: SqlExpression | str | list[SqlExpression | str]

    def __init__(self, expression: SqlExpression | str | list[SqlExpression | str]):
        super().__init__()
        self.expression = expression
        self.handle_parent_node_update(expression)


@dataclass
class SUM(SqlExpression):
    expression: SqlExpression | str

    def __init__(self, expression: SqlExpression | str):
        super().__init__()
        self.expression = expression
        self.handle_parent_node_update(expression)


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

    def __init__(
        self,
        condition: SqlExpression | str,
        if_expression: SqlExpression | str,
        else_expression: SqlExpression | str | None = None,
    ):
        super().__init__()
        self.condition = condition
        self.if_expression = if_expression
        self.else_expression = else_expression
        self.handle_parent_node_update(condition)
        self.handle_parent_node_update(if_expression)
        self.handle_parent_node_update(else_expression)


@dataclass
class TUPLE(SqlExpression):
    expressions: list[SqlExpression | str]

    def __init__(self, expressions: list[SqlExpression | str]):
        super().__init__()
        self.expressions = expressions
        self.handle_parent_node_update(expressions)


@dataclass
class IS_NULL(SqlExpression):
    expression: SqlExpression | str

    def __init__(self, expression: SqlExpression | str):
        super().__init__()
        self.expression = expression
        self.handle_parent_node_update(expression)


@dataclass
class IS_NOT_NULL(SqlExpression):
    expression: SqlExpression | str

    def __init__(self, expression: SqlExpression | str):
        super().__init__()
        self.expression = expression
        self.handle_parent_node_update(expression)


@dataclass
class IN(SqlExpression):
    expression: SqlExpression | str
    list_expression: list[SqlExpression]

    def __init__(self, expression: SqlExpression | str, list_expression: list[SqlExpression]):
        super().__init__()
        self.expression = expression
        self.list_expression = list_expression
        self.handle_parent_node_update(expression)
        self.handle_parent_node_update(list_expression)


@dataclass
class LOWER(SqlExpression):
    expression: SqlExpression | str

    def __init__(self, expression: SqlExpression | str):
        super().__init__()
        self.expression = expression
        self.handle_parent_node_update(expression)


@dataclass
class LENGTH(SqlExpression):
    expression: SqlExpression | str

    def __init__(self, expression: SqlExpression | str):
        super().__init__()
        self.expression = expression
        self.handle_parent_node_update(expression)


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

    def __init__(self, name: str, args: list[SqlExpression | str]):
        super().__init__()
        self.name = name
        self.args = args
        self.handle_parent_node_update(args)


@dataclass
class MAX(SqlExpression):
    expression: SqlExpression | str

    def __init__(self, expression: SqlExpression | str):
        super().__init__()
        self.expression = expression
        self.handle_parent_node_update(expression)


@dataclass
class LITERAL(SqlExpression):
    value: object


@dataclass
class Operator(SqlExpression):
    left: SqlExpression | str
    right: SqlExpression | str

    def __init__(self, left: SqlExpression | str, right: SqlExpression | str):
        super().__init__()
        self.left = left
        self.right = right
        self.handle_parent_node_update(left)
        self.handle_parent_node_update(right)


@dataclass
class EQ(Operator):
    def __init__(self, left: SqlExpression | str, right: SqlExpression | str):
        super().__init__(left, right)


@dataclass
class NEQ(Operator):
    def __init__(self, left: SqlExpression | str, right: SqlExpression | str):
        super().__init__(left, right)


@dataclass
class GT(Operator):
    def __init__(self, left: SqlExpression | str, right: SqlExpression | str):
        super().__init__(left, right)


@dataclass
class GTE(Operator):
    def __init__(self, left: SqlExpression | str, right: SqlExpression | str):
        super().__init__(left, right)


@dataclass
class LT(Operator):
    def __init__(self, left: SqlExpression | str, right: SqlExpression | str):
        super().__init__(left, right)


@dataclass
class LTE(Operator):
    def __init__(self, left: SqlExpression | str, right: SqlExpression | str):
        super().__init__(left, right)


@dataclass
class NOT(SqlExpression):
    expression: SqlExpression | str

    def __init__(self, expression: SqlExpression | str):
        super().__init__()
        self.expression = expression
        self.handle_parent_node_update(expression)

    @classmethod
    def optional(cls, expression: SqlExpression | str | None) -> Optional[NOT]:
        return NOT(expression) if expression is not None else None


@dataclass
class REGEX_LIKE(SqlExpression):
    expression: SqlExpression | str
    regex_pattern: str

    def __init__(self, expression: SqlExpression | str, regex_pattern: str):
        super().__init__()
        self.expression = expression
        self.regex_pattern = regex_pattern
        self.handle_parent_node_update(expression)


@dataclass
class LIKE(Operator):
    def __init__(self, left: SqlExpression | str, right: SqlExpression | str):
        super().__init__(left, right)


@dataclass
class NOT_LIKE(Operator):
    def __init__(self, left: SqlExpression | str, right: SqlExpression | str):
        super().__init__(left, right)


@dataclass
class AND(SqlExpression):
    clauses: SqlExpression | str | list[SqlExpression]

    def __init__(self, clauses: SqlExpression | str | list[SqlExpression]):
        super().__init__()
        self.clauses = clauses
        self.handle_parent_node_update(clauses)

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

    def __init__(self, clauses: SqlExpression | str | list[SqlExpression]):
        super().__init__()
        self.clauses = clauses
        self.handle_parent_node_update(clauses)

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
class ORDER_BY_ASC(BaseSqlExpression):
    """
    Multiple of these can be added to a sql select statement. They are added in order.
    """

    expression: SqlExpression | str

    def __init__(self, expression: SqlExpression | str):
        super().__init__()
        self.expression = expression
        self.handle_parent_node_update(expression)


@dataclass
class ORDER_BY_DESC(BaseSqlExpression):
    """
    Multiple of these can be added to a sql select statement. They are added in order.
    """

    expression: SqlExpression | str

    def __init__(self, expression: SqlExpression | str):
        super().__init__()
        self.expression = expression
        self.handle_parent_node_update(expression)


@dataclass
class ORDINAL_POSITION(BaseSqlExpression):
    def __init__(self):
        super().__init__()
