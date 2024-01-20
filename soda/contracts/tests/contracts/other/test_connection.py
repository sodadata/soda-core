import os
from textwrap import dedent

import pytest

from helpers.fixtures import project_root_dir
from soda.contracts.connection import Connection, SodaException


def test_connection_exception_file_not_found():
    connection = Connection.from_yaml_file("./non_existing_file.scn.yml")
    connection_logs = str(connection.logs)
    assert "file './non_existing_file.scn.yml'" in connection_logs
    assert "No such file or directory" in connection_logs


def test_connection_from_file_with_variable_resolving(environ):
    environ["POSTGRES_DATABASE"] = "sodasql"
    environ["POSTGRES_USERNAME"] = "sodasql"

    connection_file_path = f"{project_root_dir}soda/contracts/tests/contracts/other/test_connection.scn.yml"
    with Connection.from_yaml_file(connection_file_path) as connection:
        assert connection.dbapi_connection


def test_invalid_database():
    connection = Connection.from_yaml_str(dedent("""
        type: postgres
        host: localhost
        database: invalid_db
        username: sodasql
    """))
    connection_logs = str(connection.logs)

    assert "database \"invalid_db\" does not exist" in connection_logs


def test_invalid_username():
    connection = Connection.from_yaml_str(dedent("""
        type: postgres
        host: localhost
        database: sodasql
        username: invalid_usr
    """))
    connection_logs = str(connection.logs)
    assert "role \"invalid_usr\" does not exist" in connection_logs
