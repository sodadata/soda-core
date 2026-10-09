"""Filtered CTE aliases and construction shared by the base scope and declared scopes."""

from __future__ import annotations

import pytest
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.filtered_cte import (
    FILTERED_CTE_ALIAS_MAX_LENGTH,
    build_filtered_cte,
    filtered_cte_alias,
    is_filtered_cte_alias,
)
from soda_core.common.metadata_types import SamplerType
from soda_core.common.sql_ast import (
    COUNT,
    CTE,
    FROM,
    SELECT,
    SODA_FILTERED_CTE_NAME,
    STAR,
    WHERE,
    WITH,
    SqlExpressionStr,
)
from soda_core.common.sql_dialect import SqlDialect


@pytest.mark.parametrize(
    "key, alias",
    [
        (None, "_soda_filtered_dataset"),
        ("eu", "_soda_filtered_scope_eu"),
        ("eu_west", "_soda_filtered_scope_eu_west"),
        ("abcdefghi", "_soda_filtered_scope_abcdefghi"),
        ("eu-west", "_soda_filtered_scope__c3b6b924"),
        ("abcdefghij", "_soda_filtered_scope__6366d953"),
        ("north_america", "_soda_filtered_scope__d1fa6fda"),
    ],
)
def test_filtered_cte_alias(key, alias):
    assert filtered_cte_alias(key) == alias


@pytest.mark.parametrize("key", ["eu", "eu_west", "eu-west", "abcdefghi", "abcdefghij", "a" * 64, "a-" * 32])
def test_filtered_cte_alias_fits_the_oracle_limit(key):
    assert len(filtered_cte_alias(key)) <= FILTERED_CTE_ALIAS_MAX_LENGTH


@pytest.mark.parametrize(
    "alias",
    [
        "_soda_filtered_dataset",
        "_soda_filtered_source_dataset",
        "_soda_filtered_source_My_Source",
        "_soda_filtered_source_a1b2c3d4",
        "_soda_filtered_target_dataset",
        "_soda_filtered_scope_eu",
        "_soda_filtered_scope__c3b6b924",
    ],
)
def test_is_filtered_cte_alias_accepts(alias):
    assert is_filtered_cte_alias(alias)


@pytest.mark.parametrize(
    "alias",
    [
        "_soda_filtered_referenced_dataset",
        "_soda_filtered_dataset_x",
        "_SODA_FILTERED_DATASET",
        "_soda_filtered_scope",
        "_soda_filtered_",
        "failed_rows",
        "cte",
        "sampled_table",
        "mins",
    ],
)
def test_is_filtered_cte_alias_rejects(alias):
    assert not is_filtered_cte_alias(alias)


def _origin_cte(dataset_identifier: DatasetIdentifier, filter: str | None) -> CTE:
    return CTE(SODA_FILTERED_CTE_NAME).AS(
        [
            SELECT(STAR()),
            FROM(dataset_identifier.dataset_name, dataset_identifier.prefixes),
            WHERE.optional(SqlExpressionStr.optional(filter)),
        ]
    )


def test_build_filtered_cte_uses_the_given_alias():
    dataset_identifier = DatasetIdentifier.parse("ds/db/schema/table")
    cte = build_filtered_cte(dataset_identifier, None, filtered_cte_alias("eu"))
    assert cte.alias == "_soda_filtered_scope_eu"


def _render(sql_dialect: SqlDialect, cte: CTE) -> str:
    return sql_dialect.build_select_sql([WITH([cte]), SELECT(COUNT(STAR())), FROM(cte.alias)])


@pytest.mark.parametrize("filter", [None, "id > 1"])
def test_build_filtered_cte_renders_the_origin_sql(filter):
    dataset_identifier = DatasetIdentifier.parse("ds/db/schema/table")
    sql = _render(SqlDialect(), build_filtered_cte(dataset_identifier, filter, SODA_FILTERED_CTE_NAME))
    assert sql == _render(SqlDialect(), _origin_cte(dataset_identifier, filter))
    assert f'"{SODA_FILTERED_CTE_NAME}" AS (' in sql


def test_build_filtered_cte_with_sampler_renders_the_sampled_origin_sql():
    # The base dialect cannot render a sample, so the sampled SQL is rendered for Postgres.
    postgres = pytest.importorskip("soda_postgres.common.data_sources.postgres_data_source")
    sql_dialect = postgres.PostgresSqlDialect()
    dataset_identifier = DatasetIdentifier.parse("ds/db/schema/table")
    expected = _origin_cte(dataset_identifier, "id > 1")
    expected.cte_query[1] = expected.cte_query[1].SAMPLE(SamplerType.PERCENTAGE, 10)

    sql = _render(
        sql_dialect,
        build_filtered_cte(dataset_identifier, "id > 1", SODA_FILTERED_CTE_NAME, sampler=(SamplerType.PERCENTAGE, 10)),
    )

    assert sql == _render(sql_dialect, expected)
    assert "TABLESAMPLE BERNOULLI(10)" in sql
    assert sql != _render(sql_dialect, _origin_cte(dataset_identifier, "id > 1"))
