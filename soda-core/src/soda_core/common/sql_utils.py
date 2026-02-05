from __future__ import annotations

from numbers import Number

import sqlglot
from soda_core.common.sql_ast import SamplerType
from sqlglot import exp


def build_sample_clause(sampler_limit: Number, sampler_type: SamplerType) -> exp.TableSample:
    if sampler_limit <= 0:
        raise ValueError("sampler_limit must be positive")

    size = exp.Literal.number(sampler_limit)
    sample = exp.TableSample()

    if sampler_type == SamplerType.ABSOLUTE_LIMIT:
        sample.set("size", size)
    else:
        raise ValueError(f"Unsupported sample type: {sampler_type}")

    return sample


def attach_sample_to_relation(rel: exp.Expression, sampler_limit: Number, sampler_type: SamplerType) -> None:
    """
    Attach a TableSample clause to a relation (Table or Subquery),
    unless it already has one.
    """
    if rel is None:
        return

    if rel.args.get("sample"):
        return

    if isinstance(rel, (exp.Table, exp.Subquery)):
        rel.set("sample", build_sample_clause(sampler_limit, sampler_type))


def apply_sampling_to_sql(
    sql: str,
    sampler_limit: Number,
    sampler_type: SamplerType,
    read_dialect: str | None = None,
    write_dialect: str | None = None,
) -> str:
    """
    Add TABLESAMPLE / SAMPLE to every table-like source in all FROM and JOIN clauses,
    including inside CTEs and subqueries.

    Exact rendering is dialect-specific.
    """
    tree = sqlglot.parse_one(sql, read=read_dialect) if read_dialect else sqlglot.parse_one(sql)

    # FROM sources (top-level, CTE bodies, nested subqueries)
    # Keep track of CTEs and skip them as they are already sampled at their definition
    # Skip subqueries as they are sampled at their definition. We skip the FROM SUBQUERY part, the FROM within the subquery is handled when we process that subquery separately.
    ctes = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}
    for from_ in tree.find_all(exp.From):
        if isinstance(from_.this, exp.Table) and from_.this.alias_or_name in ctes:
            continue
        if isinstance(from_.this, exp.Subquery):
            continue

        attach_sample_to_relation(from_.this, sampler_limit, sampler_type)

    # JOIN targets
    for join in tree.find_all(exp.Join):
        attach_sample_to_relation(join.this, sampler_limit, sampler_type)

    return tree.sql(dialect=write_dialect, pretty=True) if write_dialect else tree.sql(pretty=True)
