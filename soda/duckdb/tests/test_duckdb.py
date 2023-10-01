from pathlib import Path
from helpers.data_source_fixture import DataSourceFixture
from soda.duckdb.soda.data_sources.duckdb_data_source import DuckDBDataSource


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
    import logging
    import pandas as pd
    from soda.common.logs import Logs
    
    csv_folder = tmp_path / "csv"
    csv_folder.mkdir()
    csv_path = csv_folder / "dataset.csv"
    
    test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
    test_df.to_csv(csv_path)
    
    data_source_properties = {
      "path": csv_path,
    }

    data_source = DuckDBDataSource(logs=Logs(logging.getLogger("test_df_from_csv")), 
                                   data_source_name="dataset",
                                   data_source_properties=data_source_properties)
    
    scan = data_source.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for dataset:
            - row_count = 4
            - missing_count(i) = 0
            - missing_count(j) = 0
        """
    )
    scan.execute(allow_warnings_only=True)
    scan.assert_all_checks_pass()
    scan.assert_no_error_logs()
    
def test_df_from_json(data_source_fixture: DataSourceFixture, tmp_path: Path):
    import logging
    import pandas as pd
    from soda.common.logs import Logs
    
    json_folder = tmp_path / "json"
    json_folder.mkdir()
    json_path = csv_folder / "dataset.csv"
    
    test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})
    test_df.to_csv(json_path)
    
    data_source_properties = {
      "path": json_path,
    }

    data_source = DuckDBDataSource(logs=Logs(logging.getLogger("test_df_from_csv")), 
                                   data_source_name="dataset",
                                   data_source_properties=data_source_properties)
    
    scan = data_source.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for dataset:
            - row_count = 4
            - missing_count(i) = 0
            - missing_count(j) = 0
        """
    )
    scan.execute(allow_warnings_only=True)
    scan.assert_all_checks_pass()
    scan.assert_no_error_logs()