import pytest
import time
from helpers.test_connection import TestConnection
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl


test_connections: list[TestConnection] = [
    TestConnection(  # correct connection, should work
        test_name="in_memory_connection",
        connection_yaml_str=f"""
                type: duckdb
                name: DUCKDB_TEST
                connection:
                    database: ":memory:db1"  # unique name so it doesn't interfere with other tests
            """,
    ),
    TestConnection(  # correct connection, should work
        test_name="file_connection",
        connection_yaml_str=f"""
                type: duckdb
                name: DUCKDB_TEST
                connection:
                    database: "test.db"
            """,
    ),
    TestConnection(  # test read_only param is applied
        test_name="read_only_applied",
        connection_yaml_str=f"""
                type: duckdb
                name: DUCKDB_TEST
                connection:
                    database: ":memory:db2"  # unique name so it doesn't interfere with other tests
                    read_only: true
            """,
        valid_connection_params=False,
        expected_connection_error="Cannot launch in-memory database in read-only mode"
    )
]


# run tests.  parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_duckdb_connections(test_connection: TestConnection):        
    test_connection.test()
        

def test_connection_reused_in_memory():
    """Test that in-memory databases are re-used if they exist."""
    
    # set up an initial connection
    test_connection = TestConnection(
        test_name="connection_reused",
        connection_yaml_str=f"""
                type: duckdb
                name: DUCKDB_TEST
                connection:
                    database: ":memory:"
            """
    )
    data_source_yaml = test_connection.create_data_source_yaml()
    data_source_impl = test_connection.create_data_source_impl(data_source_yaml)
    data_source_impl.open_connection()
    conn = data_source_impl.data_source_connection
    # write a table - this should persist in memory
    conn.execute_query("Create table foo as (select 123)")
    # connection remains open

    # create a second connection which should re-use the first 
    # so long as it refers to ":memory:"
    test_connection2 = TestConnection(
        test_name="connection_reused_2",
        connection_yaml_str=f"""
                type: duckdb 
                name: DUCKDB_TEST
                connection:
                    database: ":memory:"
            """
    )
    data_source_yaml = test_connection2.create_data_source_yaml()
    data_source_impl = test_connection2.create_data_source_impl(data_source_yaml)
    data_source_impl.open_connection()
    conn = data_source_impl.data_source_connection
    assert conn.execute_query("select * from foo").rows == [(123,)]
    data_source_impl.close_connection()


def test_connection_from_existing_cursor():
    # Test passing an existing duckdb connection 
    import duckdb
    conn = duckdb.connect(":memory:db3")
    conn.sql("Create table foo as (select 456)")
    new_conn = DuckDBDataSourceImpl.from_existing_cursor(conn, "DUCKDB_TEST")
    new_conn.open_connection()
    assert new_conn.execute_query("select * from foo").rows == [(456,)]
    new_conn.close_connection()
    