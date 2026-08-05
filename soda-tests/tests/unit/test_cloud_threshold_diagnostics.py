from soda_core.common.soda_cloud import _build_diagnostics_json_dict
from soda_core.contracts.contract_verification import (
    Check,
    CheckOutcome,
    CheckResult,
    Threshold,
)


def build_check_result(threshold: Threshold, warn_threshold: Threshold = None) -> CheckResult:
    check = Check(
        column_name=None,
        type="row_count",
        qualifier=None,
        name="Row count meets expected threshold",
        relative_path="checks.row_count",
        identity="abc123",
        definition="checks:\n- row_count:\n",
        contract_file_line=1,
        contract_file_column=1,
        threshold=threshold,
        attributes={},
        location=None,
        warn_threshold=warn_threshold,
    )
    return CheckResult(
        check=check,
        outcome=CheckOutcome.PASSED,
        threshold_value=50,
        # The "v4" sub-dict reads these unconditionally; mirrors how the other
        # soda_cloud unit tests construct a CheckResult.
        diagnostic_metric_values={"check_rows_tested": 0, "dataset_rows_tested": 0},
    )


def test_fail_and_warn_blocks_emitted():
    diagnostics = _build_diagnostics_json_dict(
        build_check_result(
            threshold=Threshold(level="fail", must_be_greater_than=10),
            warn_threshold=Threshold(level="warn", must_be_greater_than=100),
        )
    )
    assert diagnostics["fail"] == {
        "greaterThan": None,
        "greaterThanOrEqual": None,
        "lessThan": None,
        "lessThanOrEqual": 10,
    }
    assert diagnostics["warn"] == {
        "greaterThan": None,
        "greaterThanOrEqual": None,
        "lessThan": None,
        "lessThanOrEqual": 100,
    }


def test_legacy_warn_level_threshold_bounds_move_to_warn_key():
    diagnostics = _build_diagnostics_json_dict(
        build_check_result(threshold=Threshold(level="warn", must_be_greater_than=100))
    )
    assert diagnostics["fail"] is None
    assert diagnostics["warn"] == {
        "greaterThan": None,
        "greaterThanOrEqual": None,
        "lessThan": None,
        "lessThanOrEqual": 100,
    }


def test_fail_only_unchanged():
    diagnostics = _build_diagnostics_json_dict(
        build_check_result(threshold=Threshold(level="fail", must_be_greater_than=0))
    )
    assert diagnostics["fail"] == {
        "greaterThan": None,
        "greaterThanOrEqual": None,
        "lessThan": None,
        "lessThanOrEqual": 0,
    }
    assert diagnostics["warn"] is None
