from numbers import Number

import sqlglot
from soda_core.common.soda_cloud_dto import SamplerType
from sqlglot import exp


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
    if sampler_limit <= 0:
        raise ValueError("sampler_limit must be positive")

    tree = sqlglot.parse_one(sql, read=read_dialect) if read_dialect else sqlglot.parse_one(sql)

    def build_sample_clause() -> exp.TableSample:
        size = exp.Literal.number(sampler_limit)
        sample = exp.TableSample()

        if sampler_type == SamplerType.ABSOLUTE_LIMIT:
            sample.set("size", size)
        else:
            raise ValueError(f"Unsupported sample type: {sampler_type}")

        return sample

    def attach_sample_to_relation(rel: exp.Expression) -> None:
        """
        Attach a TableSample clause to a relation (Table or Subquery),
        unless it already has one.
        """
        if rel is None:
            return

        if rel.args.get("sample"):
            return

        if isinstance(rel, (exp.Table, exp.Subquery)):
            rel.set("sample", build_sample_clause())

    # FROM sources (top-level, CTE bodies, nested subqueries)
    for from_ in tree.find_all(exp.From):
        attach_sample_to_relation(from_.this)

    # JOIN targets
    for join in tree.find_all(exp.Join):
        attach_sample_to_relation(join.this)

    return tree.sql(dialect=write_dialect) if write_dialect else tree.sql()
