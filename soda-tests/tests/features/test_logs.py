import logging

from soda_core.common.logs import Logs


def test_catch_args_in_logs_messages():
    l = Logs()
    logging.debug("This is a test message 1")
    logging.warning("This is a test %s", "message 2")
    logging.error("This is a test %s", "message 3")
    l.remove_from_root_logger()
    logs = l.get_logs()
    assert len(logs) == 3, f"Expected 3 error log, got {len(logs)}"
    assert "This is a test message 1" in logs
    assert "This is a test message 2" in logs
    assert "This is a test message 2" in logs

    error_logs = l.get_errors()
    assert len(error_logs) == 1, f"Expected 1 error log, got {len(error_logs)}"
    assert "This is a test message 3" in error_logs, f"Expected error log message not found in {error_logs}"
