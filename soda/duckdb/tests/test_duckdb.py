from pathlib import Path

from helpers.data_source_fixture import DataSourceFixture


def test_pandas_df(data_source_fixture: DataSourceFixture):
    import duckdb
    import pandas as pd

    con = duckdb.connect(database=":memory:")
    test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})

    scan = data_source_fixture.create_test_scan()
    scan.add_duckdb_connection(con)
    scan.add_sodacl_yaml_str(
        f"""
          checks for test_df:
            - row_count = 4
            - missing_count(i) = 0
            - missing_count(j) = 0
        """
    )
    scan.execute(allow_warnings_only=True)
    scan.assert_all_checks_pass()
    scan.assert_no_error_logs()


def test_df_from_csv(data_source_fixture: DataSourceFixture, tmp_path: Path):
    import pandas as pd

    csv_folder = tmp_path / "csv"
    csv_folder.mkdir()
    csv_path = csv_folder / "csv_dataset.csv"

    test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
    test_df.to_csv(csv_path)

    scan = data_source_fixture.create_test_scan()
    scan.set_data_source_name("csv_dataset")
    scan.add_configuration_yaml_str(
        f"""
          data_source csv_dataset:
            type: duckdb
            path: {csv_path}
        """
    )
    scan.add_sodacl_yaml_str(
        """
          checks for csv_dataset:
            - row_count = 4
            - missing_count(i) = 0
            - missing_count(j) = 0
        """
    )
    scan.execute(allow_warnings_only=True)
    scan.assert_all_checks_pass()
    scan.assert_no_error_logs()


def test_df_from_json(data_source_fixture: DataSourceFixture, tmp_path: Path):
    import pandas as pd

    json_folder = tmp_path / "json"
    json_folder.mkdir()
    json_path = json_folder / "json_dataset.json"

    test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
    test_df.to_json(json_path)

    scan = data_source_fixture.create_test_scan()
    scan.set_data_source_name("json_dataset")
    scan.add_configuration_yaml_str(
        f"""
          data_source json_dataset:
            type: duckdb
            path: {json_path}
        """
    )
    scan.add_sodacl_yaml_str(
        """
          checks for json_dataset:
            - missing_count(i) = 0
            - missing_count(j) = 0
        """
    )
    scan.execute(allow_warnings_only=True)
    scan.assert_all_checks_pass()
    scan.assert_no_error_logs()


def test_df_from_parquet(data_source_fixture: DataSourceFixture, tmp_path: Path):
    import pandas as pd

    parquet_folder = tmp_path / "parquet"
    parquet_folder.mkdir()
    parquet_path = parquet_folder / "parquet_dataset.parquet"

    test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
    test_df.to_parquet(parquet_path)

    scan = data_source_fixture.create_test_scan()
    scan.set_data_source_name("parquet_dataset")
    scan.add_configuration_yaml_str(
        f"""
          data_source parquet_dataset:
            type: duckdb
            path: {parquet_path}
        """
    )
    scan.add_sodacl_yaml_str(
        """
          checks for parquet_dataset:
            - row_count = 4
            - missing_count(i) = 0
            - missing_count(j) = 0
        """
    )
    scan.execute(allow_warnings_only=True)
    scan.assert_all_checks_pass()
    scan.assert_no_error_logs()
