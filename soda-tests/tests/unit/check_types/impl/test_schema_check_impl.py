"""
Unit tests for SchemaCheckImpl.setup_metrics() and evaluate().

Tests verify that:
- setup_metrics() creates a SchemaMetricImpl
- expected_columns is built from the contract's column definitions
- evaluate() returns the correct CheckOutcome for various actual column scenarios

Note: Data type comparison requires data_source_impl.sql_dialect, which is not
available without a database.  These tests therefore focus on column-name-level
checks (missing columns, extra columns, allow flags) and leave data-type
mismatch testing to integration tests.
"""

from helpers.impl_test_helpers import build_contract_impl, get_check_impl
from soda_core.common.metadata_types import ColumnMetadata, SqlDataType
from soda_core.contracts.contract_verification import CheckOutcome, Measurement
from soda_core.contracts.impl.check_types.schema_check import (
    SchemaCheckImpl,
    SchemaCheckResult,
    SchemaMetricImpl,
)
from soda_core.contracts.impl.contract_verification_impl import MeasurementValues


def test_schema_setup_metrics_creates_schema_metric():
    """setup_metrics() should register a SchemaMetricImpl."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
      - name: name
        data_type: character varying
    checks:
      - schema:
    """
    check = get_check_impl(contract_yaml)
    assert isinstance(check, SchemaCheckImpl)
    schema_metrics = [m for m in check.metrics if isinstance(m, SchemaMetricImpl)]
    assert len(schema_metrics) == 1


def test_schema_expected_columns_from_contract():
    """expected_columns should match the contract column definitions."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
      - name: email
        data_type: character varying
      - name: score
        data_type: numeric
    checks:
      - schema:
    """
    check = get_check_impl(contract_yaml)
    assert isinstance(check, SchemaCheckImpl)
    expected_names = [col.column_name for col in check.expected_columns]
    assert expected_names == ["id", "email", "score"]
    assert check.expected_columns[0].sql_data_type.name == "integer"
    assert check.expected_columns[1].sql_data_type.name == "character varying"
    assert check.expected_columns[2].sql_data_type.name == "numeric"


def test_schema_expected_columns_with_character_length():
    """expected_columns should include character_maximum_length when specified."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: zip_code
        data_type: character varying
        character_maximum_length: 10
    checks:
      - schema:
    """
    check = get_check_impl(contract_yaml)
    assert check.expected_columns[0].sql_data_type.character_maximum_length == 10


def _build_schema_measurement_values(check: SchemaCheckImpl, actual_columns: list[ColumnMetadata]) -> MeasurementValues:
    """Helper to build MeasurementValues with schema metric data."""
    measurements = [
        Measurement(
            metric_id=check.schema_metric.id,
            value=actual_columns,
            metric_name=check.schema_metric.type,
        )
    ]
    return MeasurementValues(measurements)


def test_schema_evaluate_not_evaluated_when_no_actual_columns():
    """actual_columns=None (no measurement) → NOT_EVALUATED."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
    checks:
      - schema:
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]
    assert isinstance(check, SchemaCheckImpl)

    # No measurement for schema metric → get_value returns None → empty actual_columns
    mv = MeasurementValues([])
    result = check.evaluate(mv)
    assert isinstance(result, SchemaCheckResult)
    assert result.outcome == CheckOutcome.NOT_EVALUATED


def test_schema_evaluate_passes_when_columns_match():
    """Actual columns match expected → PASSED."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
      - name: name
        data_type: character varying
    checks:
      - schema:
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]

    actual_columns = [
        ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="integer")),
        ColumnMetadata(column_name="name", sql_data_type=SqlDataType(name="character varying")),
    ]
    mv = _build_schema_measurement_values(check, actual_columns)
    result = check.evaluate(mv)
    assert result.outcome == CheckOutcome.PASSED
    assert result.expected_column_names_not_actual == []
    assert result.actual_column_names_not_expected == []
    assert result.diagnostic_metric_values == {"schema_events_count": 0}


def test_schema_evaluate_fails_when_column_missing():
    """Expected column not in actual → FAILED."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
      - name: name
        data_type: character varying
    checks:
      - schema:
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]

    # Only "id" present, "name" missing
    actual_columns = [
        ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="integer")),
    ]
    mv = _build_schema_measurement_values(check, actual_columns)
    result = check.evaluate(mv)
    assert result.outcome == CheckOutcome.FAILED
    assert "name" in result.expected_column_names_not_actual


def test_schema_evaluate_fails_when_extra_column():
    """Actual column not in expected (allow_extra_columns=False) → FAILED."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
    checks:
      - schema:
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]

    actual_columns = [
        ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="integer")),
        ColumnMetadata(column_name="extra_col", sql_data_type=SqlDataType(name="text")),
    ]
    mv = _build_schema_measurement_values(check, actual_columns)
    result = check.evaluate(mv)
    assert result.outcome == CheckOutcome.FAILED
    assert "extra_col" in result.actual_column_names_not_expected


def test_schema_evaluate_passes_with_allow_extra_columns():
    """Extra actual column with allow_extra_columns=True → PASSED."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
    checks:
      - schema:
          allow_extra_columns: true
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]

    actual_columns = [
        ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="integer")),
        ColumnMetadata(column_name="extra_col", sql_data_type=SqlDataType(name="text")),
    ]
    mv = _build_schema_measurement_values(check, actual_columns)
    result = check.evaluate(mv)
    assert result.outcome == CheckOutcome.PASSED
    assert result.actual_column_names_not_expected == []


def test_schema_evaluate_schema_events_count():
    """SchemaCheckResult.diagnostic_metric_values should include schema_events_count."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
      - name: name
        data_type: character varying
    checks:
      - schema:
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]

    # "name" missing + "extra" unexpected = 2 schema events
    actual_columns = [
        ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="integer")),
        ColumnMetadata(column_name="extra", sql_data_type=SqlDataType(name="text")),
    ]
    mv = _build_schema_measurement_values(check, actual_columns)
    result = check.evaluate(mv)
    assert result.outcome == CheckOutcome.FAILED
    assert result.diagnostic_metric_values == {"schema_events_count": 2}
    assert result.threshold_value == 2
