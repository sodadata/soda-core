from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_datatypes import DBDataType

logger: logging.Logger = soda_logger


@dataclass
class SqlDataType:
    name: str
    character_maximum_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None
    datetime_precision: Optional[int] = None

    def __post_init__(self):
        assert isinstance(self.name, str)
        self.name = self.name.lower()

    def get_create_table_column_type(self) -> str:
        """
        Returns the SQL data type including the parameters in round brackets for usage in a CREATE TABLE statement.
        """
        if self.character_maximum_length is not None:
            return f"{self.name}({self.character_maximum_length})"
        elif self.numeric_precision is not None and self.numeric_scale is not None:
            return f"{self.name}({self.numeric_precision},{self.numeric_scale})"
        elif self.numeric_precision is not None:
            return f"{self.name}({self.numeric_precision})"
        elif self.datetime_precision is not None:
            return f"{self.name}({self.datetime_precision})"
        else:
            return self.name

    def replace_data_type_name(self, new_data_type_name: str) -> SqlDataType:
        return SqlDataType(
            name=new_data_type_name,
            character_maximum_length=self.character_maximum_length,
            numeric_precision=self.numeric_precision,
            numeric_scale=self.numeric_scale,
            datetime_precision=self.datetime_precision,
        )


class BaseSqlExpression:
    # Initialize this outside of __init__ to avoid super().__init__() in child dataclasses
    parent_node: Optional[BaseSqlExpression] = None

    def __init__(self):  # Technically not needed, but it's here for clarity
        self.parent_node = None

    # Create __post_init__ to avoid errors when using super().__post_init__() in child dataclasses
    def __post_init__(self):
        pass

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

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.fields)

        # Check that the select contains a distinct and has multiple fields -> give a warning
        if isinstance(self.fields, list) and len(self.fields) > 1:
            if any(isinstance(field, DISTINCT) for field in self.fields):
                logger.warning(
                    """Found DISTINCT in a SELECT statement with multiple fields.
                               This might have unintended consequences."""
                )


@dataclass
class FROM(BaseSqlExpression):
    table_name: str
    table_prefix: Optional[list[str]] = None
    alias: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()

    def AS(self, alias: str) -> FROM:
        self.alias = alias
        return self

    def IN(self, table_prefix: str | list[str]) -> FROM:
        self.table_prefix = table_prefix if isinstance(table_prefix, list) else [table_prefix]
        return self


@dataclass
class LEFT_INNER_JOIN(FROM, BaseSqlExpression):
    on_condition: Optional[SqlExpression] = None

    def __post_init__(self):
        super().__post_init__()
        if self.on_condition is not None:
            self.ON(self.on_condition)

    def ON(self, on_condition: SqlExpression) -> FROM:
        self.on_condition = on_condition
        self.handle_parent_node_update(on_condition)
        return self


@dataclass
class JOIN(FROM, BaseSqlExpression):
    on_condition: Optional[SqlExpression] = None

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.on_condition)


@dataclass
class WHERE(BaseSqlExpression):
    condition: SqlExpression

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.condition)


@dataclass
class GROUP_BY(BaseSqlExpression):
    fields: list[SqlExpression | str]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.fields)


@dataclass
class WITH(BaseSqlExpression):
    alias: str
    alias_columns: list[COLUMN] | None = None
    cte_query: list | str | VALUES | None = None

    def __post_init__(self):
        super().__post_init__()

    def AS(self, cte_query: list | str) -> WITH:
        self.cte_query = cte_query
        self.handle_parent_node_update(cte_query)
        return self


@dataclass
class VALUES(BaseSqlExpression):
    # To be used in CTE.  Use VALUES_ROW for inserts
    values: list[SqlExpression | str]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.values)


@dataclass
class SqlExpression(BaseSqlExpression):
    def __post_init__(self):
        super().__post_init__()


@dataclass
class STAR(SqlExpression):
    alias: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()


@dataclass
class COUNT(SqlExpression):
    expression: SqlExpression | str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


@dataclass
class DISTINCT(SqlExpression):
    expression: SqlExpression | str | list[SqlExpression | str]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


@dataclass
class SUM(SqlExpression):
    expression: SqlExpression | str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


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

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.condition)
        self.handle_parent_node_update(self.if_expression)
        self.handle_parent_node_update(self.else_expression)


@dataclass
class TUPLE(SqlExpression):
    expressions: list[SqlExpression | str]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expressions)


@dataclass
class IS_NULL(SqlExpression):
    expression: SqlExpression | str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


@dataclass
class IS_NOT_NULL(SqlExpression):
    expression: SqlExpression | str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


@dataclass
class IN(SqlExpression):
    expression: SqlExpression | str
    list_expression: list[SqlExpression]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)
        self.handle_parent_node_update(self.list_expression)


@dataclass
class IN_SELECT(SqlExpression):
    expression: SqlExpression | str
    nested_select_elements: list[Any]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)
        self.handle_parent_node_update(self.nested_select_elements)


@dataclass
class LOWER(SqlExpression):
    expression: SqlExpression | str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


@dataclass
class LENGTH(SqlExpression):
    expression: SqlExpression | str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


@dataclass
class COLUMN(SqlExpression):
    name: SqlExpression | str  # Use SqlExpression if you need to generate and/or rename a column dymamically, e.g. using a CASE statement
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

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.args)


@dataclass
class COALESCE(SqlExpression):
    args: list[SqlExpression | str]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.args)


@dataclass
class CAST(SqlExpression):
    expression: SqlExpression | str
    to_type: str | DBDataType

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


@dataclass
class MAX(SqlExpression):
    expression: SqlExpression | str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


@dataclass
class LITERAL(SqlExpression):
    value: object


@dataclass
class Operator(SqlExpression):
    left: SqlExpression | str
    right: SqlExpression | str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.left)
        self.handle_parent_node_update(self.right)


@dataclass
class EQ(Operator):
    def __post_init__(self):
        super().__post_init__()


@dataclass
class NEQ(Operator):
    def __post_init__(self):
        super().__post_init__()


@dataclass
class GT(Operator):
    def __post_init__(self):
        super().__post_init__()


@dataclass
class GTE(Operator):
    def __post_init__(self):
        super().__post_init__()


@dataclass
class LT(Operator):
    def __post_init__(self):
        super().__post_init__()


@dataclass
class LTE(Operator):
    def __post_init__(self):
        super().__post_init__()


@dataclass
class EXISTS(SqlExpression):
    nested_select_elements: list[Any]

    def __post_init__(self):
        super().__post_init__()


@dataclass
class NOT(SqlExpression):
    expression: SqlExpression | str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)

    @classmethod
    def optional(cls, expression: SqlExpression | str | None) -> Optional[NOT]:
        return NOT(expression) if expression is not None else None


@dataclass
class REGEX_LIKE(SqlExpression):
    expression: SqlExpression | str
    regex_pattern: str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


@dataclass
class LIKE(Operator):
    def __post_init__(self):
        super().__post_init__()


@dataclass
class NOT_LIKE(Operator):
    def __post_init__(self):
        super().__post_init__()


@dataclass
class AND(SqlExpression):
    clauses: SqlExpression | str | list[SqlExpression]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.clauses)

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

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.clauses)

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

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


@dataclass
class ORDER_BY_DESC(BaseSqlExpression):
    """
    Multiple of these can be added to a sql select statement. They are added in order.
    """

    expression: SqlExpression | str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


@dataclass
class ORDINAL_POSITION(BaseSqlExpression):
    def __post_init__(self):
        super().__post_init__()


@dataclass
class CREATE_TABLE(BaseSqlExpression):
    fully_qualified_table_name: str
    columns: list[CREATE_TABLE_COLUMN]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.columns)


@dataclass
class CREATE_TABLE_IF_NOT_EXISTS(CREATE_TABLE):
    def __post_init__(self):
        super().__post_init__()


@dataclass
class CREATE_TABLE_COLUMN(BaseSqlExpression):
    name: str
    type: SqlDataType
    nullable: Optional[bool] = None
    default: Optional[SqlExpression | str] = None

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.default)

    def convert_to_standard_column(
        self, table_alias: Optional[str] = None, field_alias: Optional[str] = None
    ) -> COLUMN:
        return COLUMN(name=self.name, table_alias=table_alias, field_alias=field_alias)


@dataclass
class INSERT_INTO(BaseSqlExpression):
    fully_qualified_table_name: str
    values: list[VALUES_ROW]
    columns: list[
        COLUMN
    ]  # The order of values that is inserted should be in the same order as the columns defined here!

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.columns)
        self.handle_parent_node_update(self.values)


@dataclass
class INSERT_INTO_VIA_SELECT(BaseSqlExpression):
    fully_qualified_table_name: str
    select_elements: list[Any]  # TODO: refactor to be a single `SELECT`
    columns: list[
        COLUMN
    ]  # The order of values that is inserted should be in the same order as the columns defined here!

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.select_elements)
        self.handle_parent_node_update(self.columns)


@dataclass
class VALUES_ROW(BaseSqlExpression):
    values: list[SqlExpression | str]  # TODO: think about what types we should restrict to (e.g. `LITERAL`?)

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.values)


@dataclass
class DROP_TABLE(BaseSqlExpression):
    fully_qualified_table_name: str

    def __post_init__(self):
        super().__post_init__()


@dataclass
class DROP_TABLE_IF_EXISTS(DROP_TABLE):
    def __post_init__(self):
        super().__post_init__()
