import os

import psycopg
import pytest
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.yaml import DataSourceYamlSource

# A transaction-mode pooler with one server connection, such as the pgbouncer service in
# soda-supabase/docker-compose.yml, or the pgbouncer service of the supabase lane in CI.
SUPABASE_POOLER_PORT = os.getenv("SUPABASE_POOLER_PORT")

pytestmark = pytest.mark.skipif(
    SUPABASE_POOLER_PORT is None,
    reason="needs a transaction-mode pooler: start soda-supabase/docker-compose.yml and set SUPABASE_POOLER_PORT=6543",
)

REPEATED_QUERY = "SELECT count(*) FROM pg_catalog.pg_class"
RUNS_PER_CONNECTION = 7


def _pooled_data_source_impl(data_source_type: str) -> DataSourceImpl:
    yaml_str = f"""
        type: {data_source_type}
        name: pooled_{data_source_type}
        connection:
            host: {os.getenv("SUPABASE_POOLER_HOST", "localhost")}
            port: {SUPABASE_POOLER_PORT}
            database: {os.getenv("SUPABASE_DATABASE", "soda_test")}
            user: {os.getenv("SUPABASE_USERNAME", "soda_test")}
            password: {os.getenv("SUPABASE_POOLER_PASSWORD", "soda_test")}
            sslmode: {os.getenv("SUPABASE_SSLMODE", "prefer")}
    """
    return DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(yaml_str=yaml_str))


def _repeat_query_on_two_connections_in_turn(data_source_impl: DataSourceImpl) -> None:
    for _ in range(2):
        connection = data_source_impl.create_additional_connection()
        try:
            for _ in range(RUNS_PER_CONNECTION):
                connection.execute_query(REPEATED_QUERY)
            connection.commit()
        finally:
            connection.close_connection()


def test_supabase_repeats_a_query_through_a_transaction_pooler():
    _repeat_query_on_two_connections_in_turn(_pooled_data_source_impl("supabase"))


def test_postgres_prepared_statements_break_through_a_transaction_pooler():
    with pytest.raises(psycopg.errors.DuplicatePreparedStatement):
        _repeat_query_on_two_connections_in_turn(_pooled_data_source_impl("postgres"))
