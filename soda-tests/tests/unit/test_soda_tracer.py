from unittest import mock

from soda_core.telemetry.soda_tracer import get_decorators


def test_get_decorators_does_not_print_ast_arguments(capsys):
    def decorated_function():
        return None

    decorated_function = mock.patch("os.getcwd")(decorated_function)

    with mock.patch("soda_core.telemetry.soda_tracer.logger.debug") as debug_mock:
        decorators = get_decorators(decorated_function)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert decorators["decorated_function"]["mock"] == [{"patch"}]
    debug_mock.assert_called_once()
