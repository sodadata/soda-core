from soda.contracts.contract import Threshold, Range


def test_numeric_threshold_fail_when_greater_than_and_less_than():
    assert Threshold(greater_than=0, less_than=1).get_sodacl_threshold() == "between (0 and 1)"


def test_numeric_threshold_fail_when_greater_than_or_equal_and_less_than_or_equal():
    assert Threshold(greater_than_or_equal=0, less_than_or_equal=1).get_sodacl_threshold() == "between 0 and 1"


def test_numeric_threshold_fail_when_greater_than_and_less_than_or_equal():
    assert Threshold(greater_than=0, less_than_or_equal=1).get_sodacl_threshold() == "between (0 and 1"


def test_numeric_threshold_dfail_when_greater_than_or_equal_and_less_than():
    assert Threshold(greater_than_or_equal=0, less_than=1).get_sodacl_threshold() == "between 0 and 1)"


def test_numeric_threshold_fail_when_greater_than_and_less_than_swap():
    assert Threshold(greater_than=1, less_than=0).get_sodacl_threshold() == "not between 0 and 1"


def test_numeric_threshold_fail_when_greater_than_or_equal_and_less_than_swap():
    assert Threshold(greater_than_or_equal=1, less_than=0).get_sodacl_threshold() == "not between 0 and 1)"


def test_numeric_threshold_fail_when_greater_than_and_less_than_or_equal_swap():
    assert Threshold(greater_than=1, less_than_or_equal=0).get_sodacl_threshold() == "not between (0 and 1"


def test_numeric_threshold_fail_when_greater_than_or_equal_and_less_than_or_equal_swap():
    assert (
            Threshold(greater_than_or_equal=1, less_than_or_equal=0).get_sodacl_threshold()
            == "not between (0 and 1)"
    )


def test_numeric_threshold_fail_when_between():
    assert Threshold(between=Range(0, 1)).get_sodacl_threshold() == "between 0 and 1"


def test_numeric_threshold_fail_when_not_between():
    assert Threshold(not_between=Range(0, 1)).get_sodacl_threshold() == "not between 0 and 1"


def test_numeric_threshold_fail_when_less_than():
    assert Threshold(less_than=0).get_sodacl_threshold() == "< 0"


def test_numeric_threshold_fail_when_less_than_or_equal():
    assert Threshold(less_than_or_equal=0).get_sodacl_threshold() == "<= 0"


def test_numeric_threshold_fail_when_greater_than():
    assert Threshold(greater_than=0).get_sodacl_threshold() == "> 0"


def test_numeric_threshold_fail_when_greater_than_or_equal():
    assert Threshold(greater_than_or_equal=0).get_sodacl_threshold() == ">= 0"


def test_numeric_threshold_fail_when_equal():
    assert Threshold(equal=0).get_sodacl_threshold() == "= 0"


def test_numeric_threshold_fail_when_not_equal():
    assert Threshold(not_equal=0).get_sodacl_threshold() == "!= 0"
