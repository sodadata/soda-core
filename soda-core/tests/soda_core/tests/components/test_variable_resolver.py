from textwrap import dedent
from typing import Optional

from soda_core.common.yaml import VariableResolver


def resolve(template: str, variables: Optional[dict] = None):
    template_str_dedented = dedent(template).strip()
    return VariableResolver.resolve(source_text=template_str_dedented, variables=variables)


def test_variable():
    assert "host: test.soda.io" == resolve(
        template="""
            host: ${var.HOST}
        """,
        variables={"HOST": "test.soda.io"},
    )


def test_variable_with_spaces():
    assert "host: test.soda.io" == resolve(
        template="""
            host: ${  var.HOST }
        """,
        variables={"HOST": "test.soda.io"},
    )


def test_variable_env_var(env_vars: dict):
    env_vars["HOST"] = "test.soda.io"

    assert "host: test.soda.io" == resolve(
        template="""
            host: ${env.HOST}
        """,
        variables=None,
    )


def test_variable_not_found(env_vars: dict):
    env_vars["HOST"] = "test.soda.io"

    assert "host: ${var.HOST}" == resolve(
        template="""
            host: ${var.HOST}
        """,
        variables=None,
    )


def test_env_var_not_found(env_vars: dict):
    assert "host: ${env.HOST}" == resolve(
        template="""
            host: ${env.HOST}
        """,
        variables={"HOST": "test.soda.io"},
    )
