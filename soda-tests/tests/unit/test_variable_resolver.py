from textwrap import dedent
from typing import Optional

from soda_core.common.logs import Logs
from soda_core.common.yaml import DataSourceYamlSource, VariableResolver


def resolve(template: str, variables: Optional[dict] = None):
    template_str_dedented = dedent(template).strip()
    return VariableResolver.resolve(source_text=template_str_dedented, variable_values=variables)


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


def parsed_data_source(yaml_str: str, secret_values: Optional[dict] = None) -> dict:
    return (
        DataSourceYamlSource.from_str(dedent(yaml_str).strip()).parse_and_resolve(secret_values=secret_values).yaml_dict
    )


def test_secret():
    assert {"password": "pw", "token": "pw"} == parsed_data_source(
        """
            password: ${secret.DB_PASSWORD}
            token: ${ secret.DB_PASSWORD }
        """,
        secret_values={"DB_PASSWORD": "pw"},
    )


def test_a_secret_with_text_around_it_is_refused(logs: Logs):
    assert {
        "url": "https://${secret.DB_PASSWORD}.attacker.example",
        "user": "alice:${secret.DB_PASSWORD}",
        "two": "${secret.DB_PASSWORD}${secret.DB_PASSWORD}",
        "with_env": "${env.HOST}${secret.DB_PASSWORD}",
    } == parsed_data_source(
        """
            url: https://${secret.DB_PASSWORD}.attacker.example
            user: alice:${secret.DB_PASSWORD}
            two: ${secret.DB_PASSWORD}${secret.DB_PASSWORD}
            with_env: ${env.HOST}${secret.DB_PASSWORD}
        """,
        secret_values={"DB_PASSWORD": "pw"},
    )
    assert 4 == logs.get_errors_str().count("A secret must be the whole value")


def test_a_secret_name_is_anything_without_whitespace_as_in_soda_cloud():
    assert {"password": "pw"} == parsed_data_source(
        "password: ${secret.prod-db.password}", secret_values={"prod-db.password": "pw"}
    )


def test_a_secret_holding_yaml_syntax_stays_one_value():
    assert {"password": "a: [b, #c", "port": 5432} == parsed_data_source(
        """
            password: ${secret.DB_PASSWORD}
            port: 5432
        """,
        secret_values={"DB_PASSWORD": "a: [b, #c"},
    )


def test_a_secret_value_is_not_resolved_again(env_vars: dict):
    env_vars["HOST"] = "test.soda.io"

    assert {"password": "${env.HOST}"} == parsed_data_source(
        "password: ${secret.DB_PASSWORD}", secret_values={"DB_PASSWORD": "${env.HOST}"}
    )


def test_a_secret_not_provided_is_logged_by_name(logs: Logs):
    assert {"password": None} == parsed_data_source("password: ${secret.DB_PASSWORD}")
    assert "Secret 'DB_PASSWORD' was used and not provided" in logs.get_errors_str()


def test_finds_the_variable_names_of_one_namespace_in_the_values():
    yaml_dict = (
        DataSourceYamlSource.from_str(
            dedent(
                """
            password: ${secret.DB_PASSWORD}
            # user: ${secret.IN_A_COMMENT}
            hosts:
              - ${secret.prod-host}
              - ${env.PORT}
              - ${var.HOST}
            """
            ).strip()
        )
        .parse()
        .yaml_dict
    )

    assert {"DB_PASSWORD", "prod-host"} == VariableResolver.find_variable_names(yaml_dict, "secret")
    assert {"PORT"} == VariableResolver.find_variable_names(yaml_dict, "env")
