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


def test_schema_expected_columns_with_numeric_precision():
    """expected_columns should include numeric_precision and numeric_scale when specified."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: score
        data_type: numeric
        numeric_precision: 10
        numeric_scale: 2
    checks:
      - schema:
    """
    check = get_check_impl(contract_yaml)
    assert check.expected_columns[0].sql_data_type.numeric_precision == 10
    assert check.expected_columns[0].sql_data_type.numeric_scale == 2


def test_schema_expected_columns_with_datetime_precision():
    """expected_columns should include datetime_precision when specified."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: created_at
        data_type: timestamp
        datetime_precision: 6
    checks:
      - schema:
    """
    check = get_check_impl(contract_yaml)
    assert check.expected_columns[0].sql_data_type.datetime_precision == 6


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


def test_schema_evaluate_fails_when_column_missing_and_extra_columns_allowed():
    """allow_extra_columns only tolerates extra actual columns — a contract
    column missing from the actual schema must still fail."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
      - name: account_status
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
    assert result.outcome == CheckOutcome.FAILED
    assert result.expected_column_names_not_actual == ["account_status"]
    assert result.actual_column_names_not_expected == []


def test_schema_evaluate_rename_reports_missing_and_extra():
    """A renamed column surfaces as old-name-missing plus new-name-extra."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
      - name: account_status
    checks:
      - schema:
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]

    actual_columns = [
        ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="integer")),
        ColumnMetadata(column_name="account_status_new", sql_data_type=SqlDataType(name="integer")),
    ]
    mv = _build_schema_measurement_values(check, actual_columns)
    result = check.evaluate(mv)
    assert result.outcome == CheckOutcome.FAILED
    assert result.expected_column_names_not_actual == ["account_status"]
    assert result.actual_column_names_not_expected == ["account_status_new"]
    assert result.diagnostic_metric_values == {"schema_events_count": 2}


def test_schema_evaluate_rename_fails_even_with_allow_extra_columns():
    """With allow_extra_columns the new name is tolerated, but the rename still
    fails because the old name is missing."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
      - name: account_status
    checks:
      - schema:
          allow_extra_columns: true
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]

    actual_columns = [
        ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="integer")),
        ColumnMetadata(column_name="account_status_new", sql_data_type=SqlDataType(name="integer")),
    ]
    mv = _build_schema_measurement_values(check, actual_columns)
    result = check.evaluate(mv)
    assert result.outcome == CheckOutcome.FAILED
    assert result.expected_column_names_not_actual == ["account_status"]
    assert result.actual_column_names_not_expected == []


def test_schema_evaluate_column_name_matching_is_case_sensitive():
    """Characterization: column names are matched case-sensitively. A lowercase
    contract against uppercase metadata (e.g. Snowflake unquoted identifiers)
    reports every column as both missing and extra."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
      - name: name
    checks:
      - schema:
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]

    actual_columns = [
        ColumnMetadata(column_name="ID", sql_data_type=SqlDataType(name="integer")),
        ColumnMetadata(column_name="NAME", sql_data_type=SqlDataType(name="text")),
    ]
    mv = _build_schema_measurement_values(check, actual_columns)
    result = check.evaluate(mv)
    assert result.outcome == CheckOutcome.FAILED
    assert result.expected_column_names_not_actual == ["id", "name"]
    assert result.actual_column_names_not_expected == ["ID", "NAME"]


def test_schema_evaluate_out_of_order_fails_with_zero_schema_events():
    """Characterization: out-of-order columns fail the check but do not count
    as schema events — outcome is FAILED while the threshold value stays 0."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
      - name: name
    checks:
      - schema:
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]

    actual_columns = [
        ColumnMetadata(column_name="name", sql_data_type=SqlDataType(name="text")),
        ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="integer")),
    ]
    mv = _build_schema_measurement_values(check, actual_columns)
    result = check.evaluate(mv)
    assert result.outcome == CheckOutcome.FAILED
    assert result.are_columns_out_of_order is True
    assert result.threshold_value == 0
    assert result.diagnostic_metric_values == {"schema_events_count": 0}


def test_schema_evaluate_out_of_order_with_extra_column_present():
    """Order detection only considers expected columns — an interleaved extra
    column does not mask a real order violation."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
      - name: name
    checks:
      - schema:
          allow_extra_columns: true
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]

    actual_columns = [
        ColumnMetadata(column_name="name", sql_data_type=SqlDataType(name="text")),
        ColumnMetadata(column_name="extra_col", sql_data_type=SqlDataType(name="text")),
        ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="integer")),
    ]
    mv = _build_schema_measurement_values(check, actual_columns)
    result = check.evaluate(mv)
    assert result.outcome == CheckOutcome.FAILED
    assert result.are_columns_out_of_order is True
    assert result.expected_column_names_not_actual == []
    assert result.actual_column_names_not_expected == []
