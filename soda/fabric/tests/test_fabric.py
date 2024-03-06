from pathlib import Path

from soda.core.tests.helpers.data_source_fixture import DataSourceFixture


def test_pandas_df(data_source_fixture: DataSourceFixture):
    import pandas as pd

    scan = DataSourceFixture.create_test_scan()

    print("Test DF")
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
