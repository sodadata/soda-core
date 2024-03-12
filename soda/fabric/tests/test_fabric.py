from pathlib import Path

from helpers.data_source_fixture import DataSourceFixture

import pandas as pd
import pyodbc


def test_connectivity(data_source_fixture: DataSourceFixture):
    print("Starting SQL Server connection test...")

    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER=localhost,1433;'
        f'DATABASE=master;'
        f'UID=sa;'
        f'PWD=YourStrong!Passw0rd;'
        f'Encrypt=No'
    )

    try:
        with pyodbc.connect(conn_str) as conn:
            print("Connected to SQL Server successfully.")

            query = "SELECT 1 AS test_col"
            df = pd.read_sql(query, conn)
            print("Query executed and loaded into DataFrame successfully.")
            print(df)

    except Exception as e:
        print(f"Error connecting to SQL Server: {e}")


def test_pandas_df(data_source_fixture: DataSourceFixture, tmp_path: Path):
    print("CSV test")
    # TODO


def test_df_from_csv(data_source_fixture: DataSourceFixture, tmp_path: Path):
    print("CSV test")
    # TODO


def test_df_from_json(data_source_fixture: DataSourceFixture, tmp_path: Path):
    print("Json test")
    # TODO


def test_df_from_parquet(data_source_fixture: DataSourceFixture, tmp_path: Path):
    print("Parquet test")
    # TODO
