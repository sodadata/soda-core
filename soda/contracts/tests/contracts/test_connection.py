import os
from textwrap import dedent

import pytest

from helpers.fixtures import project_root_dir
from soda.contracts.connection import Connection
from soda.contracts.exceptions import SodaConnectionException


def test_connection_exception_file_not_found():
    with pytest.raises(SodaConnectionException) as excinfo:
        Connection.from_yaml_file("./non_existing_file.scn.yml")

    assert "file './non_existing_file.scn.yml'" in str(excinfo.value)
    assert "No such file or directory" in str(excinfo.value)


def test_connection_from_file_with_variable_resolving(environ):
    environ["POSTGRES_DATABASE"] = "sodasql"
    environ["POSTGRES_USERNAME"] = "sodasql"

    connection_file_path = f"{project_root_dir}soda/contracts/tests/contracts/test_connection.scn.yml"
    with Connection.from_yaml_file(connection_file_path) as connection:
        assert connection.dbapi_connection


def test_invalid_database():
    with pytest.raises(SodaConnectionException) as excinfo:
        Connection.from_yaml_str(dedent("""
            type: postgres
            host: localhost
            database: invalid_db
            username: sodasql
        """))
    assert "database \"invalid_db\" does not exist" in str(excinfo.value)


def test_invalid_username():
    with pytest.raises(SodaConnectionException) as excinfo:
        Connection.from_yaml_str(dedent("""
            type: postgres
            host: localhost
            database: sodasql
            username: invalid_usr
        """))
    assert "role \"invalid_usr\" does not exist" in str(excinfo.value)
