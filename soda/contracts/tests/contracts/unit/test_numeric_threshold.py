from soda.contracts.contract import NumericThreshold, Range


def test_numeric_threshold_fail_when_greater_than_and_less_than():
    assert NumericThreshold(greater_than=0, less_than=1)._get_sodacl_checkline_threshold() == "between (0 and 1)"


def test_numeric_threshold_fail_when_greater_than_or_equal_and_less_than_or_equal():
    assert (
        NumericThreshold(greater_than_or_equal=0, less_than_or_equal=1)._get_sodacl_checkline_threshold()
        == "between 0 and 1"
    )


def test_numeric_threshold_fail_when_greater_than_and_less_than_or_equal():
    assert (
        NumericThreshold(greater_than=0, less_than_or_equal=1)._get_sodacl_checkline_threshold()
        == "between (0 and 1"
    )


def test_numeric_threshold_dfail_when_greater_than_or_equal_and_less_than():
    assert (
        NumericThreshold(greater_than_or_equal=0, less_than=1)._get_sodacl_checkline_threshold()
        == "between 0 and 1)"
    )


def test_numeric_threshold_fail_when_greater_than_and_less_than_swap():
    assert NumericThreshold(greater_than=1, less_than=0)._get_sodacl_checkline_threshold() == "not between 0 and 1"


def test_numeric_threshold_fail_when_greater_than_or_equal_and_less_than_swap():
    assert (
        NumericThreshold(greater_than_or_equal=1, less_than=0)._get_sodacl_checkline_threshold() == "not between 0 and 1)"
    )


def test_numeric_threshold_fail_when_greater_than_and_less_than_or_equal_swap():
    assert (
        NumericThreshold(greater_than=1, less_than_or_equal=0)._get_sodacl_checkline_threshold() == "not between (0 and 1"
    )


def test_numeric_threshold_fail_when_greater_than_or_equal_and_less_than_or_equal_swap():
    assert (
        NumericThreshold(greater_than_or_equal=1, less_than_or_equal=0)._get_sodacl_checkline_threshold()
        == "not between (0 and 1)"
    )


def test_numeric_threshold_fail_when_between():
    assert NumericThreshold(between=Range(0, 1))._get_sodacl_checkline_threshold() == "between 0 and 1"


def test_numeric_threshold_fail_when_not_between():
    assert NumericThreshold(not_between=Range(0, 1))._get_sodacl_checkline_threshold() == "not between 0 and 1"


def test_numeric_threshold_fail_when_less_than():
    assert NumericThreshold(less_than=0)._get_sodacl_checkline_threshold() == "< 0"


def test_numeric_threshold_fail_when_less_than_or_equal():
    assert NumericThreshold(less_than_or_equal=0)._get_sodacl_checkline_threshold() == "<= 0"


def test_numeric_threshold_fail_when_greater_than():
    assert NumericThreshold(greater_than=0)._get_sodacl_checkline_threshold() == "> 0"


def test_numeric_threshold_fail_when_greater_than_or_equal():
    assert NumericThreshold(greater_than_or_equal=0)._get_sodacl_checkline_threshold() == ">= 0"


def test_numeric_threshold_fail_when_equal():
    assert NumericThreshold(equal=0)._get_sodacl_checkline_threshold() == "= 0"


def test_numeric_threshold_fail_when_not_equal():
    assert NumericThreshold(not_equal=0)._get_sodacl_checkline_threshold() == "!= 0"
