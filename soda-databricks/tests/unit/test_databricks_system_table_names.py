"""Databricks internal objects that discovery must skip.

Databricks materializes metric views through a managed Lakeflow pipeline whose backing
objects are named ``__materialization_mat_<pipeline id>___metric_view_mat_...``. They live
in the customer's own schemas, so schema-level system filtering does not catch them; the
dialect flags them by name instead. See SCS-1451.
"""

import pytest
from soda_databricks.common.data_sources.databricks_data_source import DatabricksHiveSqlDialect, DatabricksSqlDialect


@pytest.mark.parametrize(
    "table_name, expected",
    [
        ("__materialization_mat_0a1b2c___metric_view_mat_revenue", True),
        ("__MATERIALIZATION_MAT_0A1B2C___METRIC_VIEW_MAT_REVENUE", True),
        ("__materialization_mat_", True),
        ("customers", False),
        ("materialization_mat_revenue", False),
        ("_materialization_mat_revenue", False),
        ("my__materialization_mat_copy", False),
    ],
)
def test_databricks_dialect_flags_metric_view_materializations(table_name, expected):
    assert DatabricksSqlDialect().is_system_table_name(table_name) is expected


def test_hive_dialect_inherits_the_rule():
    assert DatabricksHiveSqlDialect().is_system_table_name("__materialization_mat_x") is True
