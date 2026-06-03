from unittest.mock import MagicMock

import pytest

from soda_core.common.logs_queue import LogsQueue


def test_logs_queue_requires_an_identifier():
    with pytest.raises(ValueError):
        LogsQueue(soda_cloud=MagicMock(), stage="test_connection")
