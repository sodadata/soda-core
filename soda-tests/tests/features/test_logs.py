import logging
import os
import tempfile
from unittest.mock import patch

from soda_core.common.logging_configuration import _masked_values, _prepare_masked_file
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


def test_mask_values_in_logs_messages(caplog):
    with tempfile.TemporaryDirectory() as tmpdirname:
        masked_values_file = os.path.join(tmpdirname, "masked_values.txt")
        with open(masked_values_file, "w") as f:
            f.write("message\n")
            f.flush()
        with patch.dict("os.environ", SODA_MASKED_VALUES_FILE=masked_values_file):
            # override the config file for masked values
            _prepare_masked_file()
        assert _masked_values == {"message"}

    # verify main logger processing
    caplog.set_level(logging.DEBUG)
    logging.debug("This is a test message X")
    log_messages = [record.message for record in caplog.records]
    assert "This is a test *** X" in log_messages

    # verify internal logs processing
    l = Logs()
    logging.debug("This is a test message 1")
    logging.warning("This is a test %s", "message 2")
    logging.error("This is a test message 3")
    l.remove_from_root_logger()
    logs = l.get_logs()
    assert len(logs) == 3, f"Expected 3 error log, got {len(logs)}"
    assert "This is a test *** 1" in logs
    assert "This is a test *** 2" in logs
    assert "This is a test *** 2" in logs

    error_logs = l.get_errors()
    assert len(error_logs) == 1, f"Expected 1 error log, got {len(error_logs)}"
    assert "This is a test *** 3" in error_logs, f"Expected error log message not found in {error_logs}"
