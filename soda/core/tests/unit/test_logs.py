from soda.common.logs import Logs


def test_logs_singleton():
    logs_one = Logs()
    logs_two = Logs()
    logs_one.warning("Message")

    assert logs_one is logs_two
    assert len(logs_one.logs) == len(logs_two.logs)
