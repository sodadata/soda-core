from __future__ import annotations

import logging
from dataclasses import dataclass
from numbers import Number
from typing import Any, Optional

from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import SamplerType, SodaDataTypeName, SqlDataType
from typing_extensions import deprecated

logger: logging.Logger = soda_logger


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
    sampler_type: Optional[SamplerType] = None
    sample_size: Optional[Number] = None

    def __post_init__(self):
        super().__post_init__()

    def AS(self, alias: str) -> FROM:
        self.alias = alias
        return self

    def IN(self, table_prefix: str | list[str]) -> FROM:
        self.table_prefix = table_prefix if isinstance(table_prefix, list) else [table_prefix]
        return self

    def SAMPLE(self, sampler_type: SamplerType, sample_size: Number) -> FROM:
        self.sampler_type = sampler_type
        self.sample_size = sample_size
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

    @classmethod
    def optional(cls, condition: Optional[SqlExpression | str]) -> Optional[SqlExpression]:
        return WHERE(condition) if condition is not None else None


@dataclass
class GROUP_BY(BaseSqlExpression):
    fields: list[SqlExpression | str]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.fields)


@dataclass
class WITH(BaseSqlExpression):
    cte_list: list[CTE]


@dataclass
class CTE(BaseSqlExpression):
    alias: str
    alias_columns: list[COLUMN] | None = None
    cte_query: list | str | VALUES | None = None

    def __post_init__(self):
        super().__post_init__()

    def AS(self, cte_query: list | str) -> CTE:
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
    field_alias: Optional[str] = None

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
class COMBINED_HASH(SqlExpression):
    expressions: list[SqlExpression | str]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expressions)


@dataclass
class CONCAT(SqlExpression):
    expressions: list[SqlExpression | str]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expressions)


@dataclass
class CONCAT_WS(SqlExpression):
    separator: str
    expressions: list[SqlExpression | str]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expressions)


@dataclass
class STRING_HASH(SqlExpression):
    expression: SqlExpression | str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


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
    name: (
        SqlExpression | str
    )  # Use SqlExpression if you need to generate and/or rename a column dymamically, e.g. using a CASE statement
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
    to_type: str | SodaDataTypeName

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


@dataclass
class AVERAGE(SqlExpression):
    expression: SqlExpression | str

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
class MIN(SqlExpression):
    expression: SqlExpression | str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)


@dataclass
class LITERAL(SqlExpression):
    value: object


@dataclass
@deprecated(
    "Do not use this unless absolutely necessary. Build expressions using supported AST nodes instead of raw SQL."
)
class RAW_SQL(SqlExpression):
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

    @classmethod
    def optional(cls, expression: Optional[SqlExpression | str]) -> Optional[SqlExpression]:
        return ORDER_BY_ASC(expression) if expression else None


@dataclass
class ORDER_BY_DESC(BaseSqlExpression):
    """
    Multiple of these can be added to a sql select statement. They are added in order.
    """

    expression: SqlExpression | str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.expression)

    @classmethod
    def optional(cls, expression: Optional[SqlExpression | str]) -> Optional[SqlExpression]:
        return ORDER_BY_DESC(expression) if expression else None


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
class CREATE_TABLE_AS_SELECT(BaseSqlExpression):
    fully_qualified_table_name: str
    select_elements: list[Any]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.select_elements)


@dataclass
class INTO(BaseSqlExpression):  # Only tested and verified for SqlServer and data sources that inherit from it.
    fully_qualified_table_name: str

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
    cascade: bool = False

    def __post_init__(self):
        super().__post_init__()


@dataclass
class DROP_TABLE_IF_EXISTS(DROP_TABLE):
    def __post_init__(self):
        super().__post_init__()


@dataclass
class LIMIT(BaseSqlExpression):
    limit: int | None = None

    def __post_init__(self):
        super().__post_init__()

    @classmethod
    def optional(cls, limit: int | None) -> Optional[LIMIT]:
        return LIMIT(limit) if limit else None


@dataclass
class OFFSET(BaseSqlExpression):
    offset: int | None = None

    def __post_init__(self):
        super().__post_init__()

    @classmethod
    def optional(cls, offset: int | None) -> Optional[OFFSET]:
        return OFFSET(offset) if offset else None


@dataclass
class ALTER_TABLE(BaseSqlExpression):
    fully_qualified_table_name: str

    def __post_init__(self):
        super().__post_init__()


@dataclass
class ALTER_TABLE_ADD_COLUMN(ALTER_TABLE):
    column: CREATE_TABLE_COLUMN

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.column)


@dataclass
class ALTER_TABLE_DROP_COLUMN(ALTER_TABLE):
    column_name: str

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.column_name)


@dataclass
class CREATE_VIEW(BaseSqlExpression):
    fully_qualified_view_name: str
    select_elements: list[Any]  # | UNION | UNION_ALL

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.select_elements)


@dataclass
class DROP_VIEW(BaseSqlExpression):
    fully_qualified_view_name: str

    def __post_init__(self):
        super().__post_init__()


@dataclass
class DROP_VIEW_IF_EXISTS(DROP_VIEW):
    def __post_init__(self):
        super().__post_init__()


@dataclass
class CREATE_MATERIALIZED_VIEW(BaseSqlExpression):
    fully_qualified_view_name: str
    select_elements: list[Any]  # | UNION | UNION_ALL

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.select_elements)


@dataclass
class DROP_MATERIALIZED_VIEW(BaseSqlExpression):
    fully_qualified_view_name: str

    def __post_init__(self):
        super().__post_init__()


@dataclass
class DROP_MATERIALIZED_VIEW_IF_EXISTS(DROP_MATERIALIZED_VIEW):
    def __post_init__(self):
        super().__post_init__()


@dataclass
@deprecated(
    "Not fully supported for all datasources yet. Only tested/verified for Postgres and Redshift. Use with caution."
)
class UNION(BaseSqlExpression):
    select_elements: list[Any]

    def __post_init__(self):
        super().__post_init__()
        self.handle_parent_node_update(self.select_elements)


@dataclass
@deprecated(
    "Not fully supported for all datasources yet. Only tested/verified for Postgres and Redshift. Use with caution."
)
class UNION_ALL(UNION):
    def __post_init__(self):
        super().__post_init__()
