from datetime import date
from decimal import Decimal

import pytest
from soda_core.check_collections.base import _skip_non_numeric_threshold_value
from soda_core.common.number_conversions import is_finite_number
from soda_core.contracts.contract_verification import Check, CheckOutcome, CheckResult


def build_check_result(threshold_value, outcome: CheckOutcome = CheckOutcome.PASSED) -> CheckResult:
    check = Check(
        column_name=None,
        type="metric",
        qualifier=None,
        name="Metric check",
        relative_path="checks.metric",
        identity="abc123",
        definition="checks:\n- metric:\n",
        contract_file_line=1,
        contract_file_column=1,
        threshold=None,
        attributes={},
        location=None,
    )
    return CheckResult(check=check, outcome=outcome, threshold_value=threshold_value)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("not_a_number", id="text"),
        pytest.param("12", id="numeric_text"),
        pytest.param(date(2026, 7, 12), id="date"),
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="inf"),
        pytest.param(Decimal("NaN"), id="decimal_nan"),
    ],
)
def test_non_numeric_value_is_skipped_and_the_check_not_evaluated(value, caplog):
    check_result = build_check_result(value)

    with caplog.at_level("ERROR"):
        _skip_non_numeric_threshold_value(check_result, "checks.metric")

    assert check_result.threshold_value is None
    assert check_result.outcome == CheckOutcome.NOT_EVALUATED
    assert "Not evaluating check at path 'checks.metric'" in caplog.text


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(None, id="unmeasured"),
        pytest.param(0, id="zero"),
        pytest.param(12.5, id="float"),
        pytest.param(True, id="bool"),
    ],
)
def test_numeric_or_missing_value_is_left_alone(value, caplog):
    check_result = build_check_result(value)

    _skip_non_numeric_threshold_value(check_result, "checks.metric")

    assert check_result.threshold_value is value
    assert check_result.outcome == CheckOutcome.PASSED
    assert caplog.text == ""


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(0, True, id="int"),
        pytest.param(12.5, True, id="float"),
        pytest.param(Decimal("1.50"), True, id="decimal"),
        pytest.param(True, True, id="bool"),
        pytest.param(float("nan"), False, id="nan"),
        pytest.param(float("-inf"), False, id="inf"),
        pytest.param(Decimal("NaN"), False, id="decimal_nan"),
        pytest.param(Decimal("sNaN"), False, id="decimal_signalling_nan"),
        pytest.param(10**400, False, id="int_beyond_float_range"),
        pytest.param("12", False, id="text"),
        pytest.param(None, False, id="none"),
    ],
)
def test_is_finite_number(value, expected):
    assert is_finite_number(value) is expected
