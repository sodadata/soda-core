from soda_core.contracts.contract_verification import CheckResult


def log_format(n, verbose: bool = False) -> str:
    return CheckResult._log_console_format(n, verbose=verbose)  # We want to test the non-verbose format


def test_log_number_format():
    assert "0.099" == log_format(0.0999999)
    assert "0.0099" == log_format(0.0099999)
    assert "0.00000099" == log_format(0.00000099999)
    assert "9.99" == log_format(9.9999999)
    assert "99.99" == log_format(99.999999)
    assert "999.99" == log_format(999.99999)
    assert "99.99" == log_format(99.99)
    assert "99.9" == log_format(99.9)
    assert "99" == log_format(99)
    assert "99999999999" == log_format(99999999999)


def test_log_number_format_verbose():
    assert "0.0999999" == log_format(0.0999999, verbose=True)
    assert "0.0099999" == log_format(0.0099999, verbose=True)
    assert "9.9999e-07" == log_format(0.00000099999, verbose=True)
    assert "9.9999999" == log_format(9.9999999, verbose=True)
    assert "99.999999" == log_format(99.999999, verbose=True)
    assert "999.99999" == log_format(999.99999, verbose=True)
    assert "99.99" == log_format(99.99, verbose=True)
    assert "99.9" == log_format(99.9, verbose=True)
    assert "99" == log_format(99, verbose=True)
    assert "99999999999" == log_format(99999999999, verbose=True)
