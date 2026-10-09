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
from soda_core.common.sql_ast import CTE, FROM, SELECT, SODA_FILTERED_CTE_NAME, STAR, WHERE, SqlExpressionStr


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


@pytest.mark.parametrize("filter", [None, "id > 1"])
def test_build_filtered_cte_equals_the_origin_construction(filter):
    dataset_identifier = DatasetIdentifier.parse("ds/db/schema/table")
    cte = build_filtered_cte(dataset_identifier, filter, SODA_FILTERED_CTE_NAME)
    assert cte == _origin_cte(dataset_identifier, filter)


def test_build_filtered_cte_with_sampler_equals_the_sampled_origin_construction():
    dataset_identifier = DatasetIdentifier.parse("ds/db/schema/table")
    expected = _origin_cte(dataset_identifier, "id > 1")
    expected.cte_query[1] = expected.cte_query[1].SAMPLE(SamplerType.ABSOLUTE_LIMIT, 10)

    cte = build_filtered_cte(
        dataset_identifier, "id > 1", SODA_FILTERED_CTE_NAME, sampler=(SamplerType.ABSOLUTE_LIMIT, 10)
    )

    assert cte == expected
    assert cte != _origin_cte(dataset_identifier, "id > 1")


def test_build_filtered_cte_uses_the_given_alias():
    dataset_identifier = DatasetIdentifier.parse("ds/db/schema/table")
    cte = build_filtered_cte(dataset_identifier, None, filtered_cte_alias("eu"))
    assert cte.alias == "_soda_filtered_scope_eu"
