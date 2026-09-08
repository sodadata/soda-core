from unittest import mock

from soda_core.telemetry.soda_tracer import get_decorators


class TestDecorators:
    @staticmethod
    def capture(_argument):
        return lambda function: function


decorators = TestDecorators()


@decorators.capture("value")
def decorated_function():
    return None


def test_get_decorators_does_not_print_ast_arguments(capsys):
    with mock.patch("soda_core.telemetry.soda_tracer.logger.debug") as debug_mock:
        decorators = get_decorators(decorated_function)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert decorators["decorated_function"]["decorators"] == [{"capture"}]
    debug_mock.assert_called_once()
