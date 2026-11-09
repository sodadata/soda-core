from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.yaml import DataSourceYamlSource
from soda_postgres.common.data_sources.postgres_data_source import PostgresDataSourceImpl, PostgresSqlDialect
from soda_postgres.common.data_sources.postgres_data_source_connection import PostgresDataSourceConnection
from soda_supabase.common.data_sources.supabase_data_source import SupabaseDataSourceImpl, SupabaseSqlDialect
from soda_supabase.common.data_sources.supabase_data_source_connection import (
    SupabaseConnectionPassword,
    SupabaseConnectionPasswordFile,
    SupabaseDataSourceConnection,
)

CONNECTION_YAML = """
    host: db.example.supabase.co
    port: 6543
    database: postgres
    user: postgres.example
"""


def _impl(data_source_type: str, connection_yaml: str) -> DataSourceImpl:
    indented_connection = "\n".join(f"  {line.strip()}" for line in connection_yaml.strip().splitlines())
    yaml_str = f"type: {data_source_type}\nname: ds\nconnection:\n{indented_connection}\n"
    return DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(yaml_str=yaml_str))


@pytest.fixture
def password_file(tmp_path: Path) -> Path:
    path = tmp_path / "password"
    path.write_text("secret-from-file\n")
    return path


@pytest.fixture
def connect_calls(monkeypatch) -> list[dict]:
    calls: list[dict] = []

    def fake_connect(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(prepare_threshold=5)

    monkeypatch.setattr(
        "soda_postgres.common.data_sources.postgres_data_source_connection.psycopg.connect", fake_connect
    )
    return calls


def test_supabase_type_reuses_the_postgres_impl_and_dialect():
    impl = _impl("supabase", CONNECTION_YAML + "password: secret")

    assert isinstance(impl, SupabaseDataSourceImpl)
    assert isinstance(impl, PostgresDataSourceImpl)
    assert impl.type_name == "supabase"
    assert isinstance(impl.sql_dialect, SupabaseSqlDialect)
    assert isinstance(impl.sql_dialect, PostgresSqlDialect)


def test_password_connection_defaults_sslmode_to_require():
    connection_properties = _impl(
        "supabase", CONNECTION_YAML + "password: secret"
    ).data_source_model.connection_properties

    assert isinstance(connection_properties, SupabaseConnectionPassword)
    assert connection_properties.sslmode == "require"
    assert connection_properties.to_connection_kwargs()["sslmode"] == "require"


def test_password_file_connection_defaults_sslmode_to_require(password_file: Path):
    impl = _impl("supabase", CONNECTION_YAML + f"password_file: {password_file}")
    connection_properties = impl.data_source_model.connection_properties

    assert isinstance(connection_properties, SupabaseConnectionPasswordFile)
    assert connection_properties.sslmode == "require"


@pytest.mark.parametrize("sslmode", ["disable", "prefer", "verify-full"])
def test_sslmode_can_be_overridden(sslmode: str):
    impl = _impl("supabase", CONNECTION_YAML + f"password: secret\nsslmode: {sslmode}")

    assert impl.data_source_model.connection_properties.to_connection_kwargs()["sslmode"] == sslmode


def test_unknown_sslmode_is_rejected():
    with pytest.raises(Exception, match="sslmode"):
        _impl("supabase", CONNECTION_YAML + "password: secret\nsslmode: always")


def test_connection_without_password_is_rejected():
    with pytest.raises(Exception, match="Unknown connection structure"):
        _impl("supabase", CONNECTION_YAML)


def test_postgres_type_keeps_its_sslmode_default():
    connection_properties = _impl(
        "postgres", CONNECTION_YAML + "password: secret"
    ).data_source_model.connection_properties

    assert connection_properties.sslmode == "prefer"


def test_password_connection_turns_prepared_statements_off(connect_calls: list[dict]):
    impl = _impl("supabase", CONNECTION_YAML + "password: secret")

    connection = impl._create_data_source_connection()

    assert isinstance(connection, SupabaseDataSourceConnection)
    assert connection.connection.prepare_threshold is None
    assert connect_calls[0]["password"] == "secret"
    assert connect_calls[0]["dbname"] == "postgres"
    assert connect_calls[0]["sslmode"] == "require"


def test_password_file_connection_turns_prepared_statements_off(connect_calls: list[dict], password_file: Path):
    impl = _impl("supabase", CONNECTION_YAML + f"password_file: {password_file}")

    connection = impl._create_data_source_connection()

    assert connection.connection.prepare_threshold is None
    assert connect_calls[0]["password"] == "secret-from-file"
    assert connect_calls[0]["sslmode"] == "require"


def test_additional_connection_turns_prepared_statements_off(connect_calls: list[dict]):
    impl = _impl("supabase", CONNECTION_YAML + "password: secret")

    connection = impl.create_additional_connection()

    assert connection.connection.prepare_threshold is None


def test_prepare_threshold_from_yaml_does_not_turn_prepared_statements_back_on(connect_calls: list[dict]):
    impl = _impl("supabase", CONNECTION_YAML + "password: secret\nprepare_threshold: 2147483647")

    connection = impl._create_data_source_connection()

    assert connection.connection.prepare_threshold is None


def test_postgres_type_keeps_psycopgs_prepare_threshold(connect_calls: list[dict]):
    impl = _impl("postgres", CONNECTION_YAML + "password: secret")

    connection = impl._create_data_source_connection()

    assert type(connection) is PostgresDataSourceConnection
    assert connection.connection.prepare_threshold == 5
    assert "sslmode" in connect_calls[0] and connect_calls[0]["sslmode"] == "prefer"
