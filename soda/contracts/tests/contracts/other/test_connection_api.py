from textwrap import dedent

from helpers.fixtures import project_root_dir

from soda.contracts.data_source import Connection


def test_connection_exception_file_not_found():
    connection = Connection.from_yaml_file("./non_existing_file.scn.yml", "postgres_ds")
    connection_logs = str(connection.logs)
    assert "file './non_existing_file.scn.yml'" in connection_logs
    assert "No such file or directory" in connection_logs


def test_connection_from_file_with_variable_resolving(environ):
    environ["POSTGRES_DATABASE"] = "sodasql"
    environ["POSTGRES_USERNAME"] = "sodasql"

    connection_file_path = f"{project_root_dir}soda/contracts/tests/contracts/other/test_data_source.scn.yml"
    with Connection.from_yaml_file(connection_file_path, "postgres_ds") as connection:
        assert connection.dbapi_connection


def test_invalid_database():
    connection = Connection.from_yaml_str(
        data_sources_yaml_str=dedent(
                """
            data_sources:
              name: postgres_ds
              type: postgres
              connection:
                host: localhost
                database: invalid_db
                username: sodasql
        """
            ),
        data_source_name="postgres_ds"
    )
    connection_logs = str(connection.logs)

    assert 'database "invalid_db" does not exist' in connection_logs


def test_invalid_username():
    connection = Connection.from_yaml_str(
        dedent(
            """
        data_sources:
          name: postgres_ds
          type: postgres
          connection:
            host: localhost
            database: sodasql
            username: invalid_usr
    """
        )
    )
    connection_logs = str(connection.logs)
    assert 'role "invalid_usr" does not exist' in connection_logs
