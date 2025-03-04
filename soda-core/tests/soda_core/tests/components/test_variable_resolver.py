from textwrap import dedent

from soda_core.common.yaml import VariableResolver


def test_variable():
    src = dedent(
        """
        host: ${HOST}
    """
    ).strip()

    result = VariableResolver.resolve(source_text_with_variables=src, variables={"HOST": "test.soda.io"})
    assert "host: test.soda.io" == result


def test_variable_with_spaces():
    src = dedent(
        """
        host: ${  HOST }
    """
    ).strip()

    result = VariableResolver.resolve(source_text_with_variables=src, variables={"HOST": "test.soda.io"})
    assert "host: test.soda.io" == result


def test_variable_env_var(env_vars: dict):
    src = dedent(
        """
        host: ${HOST}
    """
    ).strip()

    env_vars["HOST"] = "test.soda.io"

    result = VariableResolver.resolve(source_text_with_variables=src)
    assert "host: test.soda.io" == result


def test_variable_not_found():
    src = dedent(
        """
        host: ${HOST}
    """
    ).strip()

    result = VariableResolver.resolve(source_text_with_variables=src)
    assert "host: ${HOST}" == result
