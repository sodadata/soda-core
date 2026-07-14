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


def test_pandas_df_externally_supplied_connection():
    """Regression test for the ``add_duckdb_connection`` workflow on duckdb >= 1.1.0.

    duckdb >= 1.1.0 defaults ``python_scan_all_frames`` to ``False``, which stops replacement
    scans from resolving a DataFrame that only lives in a caller frame. When the user supplies
    their own connection, Soda's connect-time ``config`` never reaches it, so ``DuckDBDataSource``
    must restore the setting at runtime -- otherwise the check fails with "Table ... does not exist".

    This uses a bare ``Scan`` on purpose: the shared test fixture pre-registers its own duckdb
    connection (created through the config path, which already sets the flag), so a fixture-based
    scan never exercises -- and therefore masks -- the externally-supplied-connection branch.
    """
    import duckdb
    import pandas as pd
    from soda.scan import Scan

    # Default config -> python_scan_all_frames is False on duckdb >= 1.1.0.
    con = duckdb.connect(database=":memory:")
    test_df = pd.DataFrame.from_dict({"i": [1, 2, 3, 4], "j": ["one", "two", "three", "four"]})

    scan = Scan()
    scan.set_scan_definition_name("test_pandas_df_externally_supplied_connection")
    scan.set_data_source_name("duckdb")
    scan.add_duckdb_connection(con)
    scan.add_sodacl_yaml_str(
        """
          checks for test_df:
            - row_count = 4
            - missing_count(i) = 0
            - missing_count(j) = 0
        """
    )
    scan.execute()

    scan.assert_no_error_logs()
    assert not scan.get_checks_fail(), scan.get_checks_fail_text()


def test_add_duckdb_connection_honors_configuration_override():
    """The `configuration` argument of `add_duckdb_connection` overrides the applied settings.

    By default Soda forces `python_scan_all_frames=True` on the supplied connection; a user can
    opt out (or set any other duckdb setting) by passing `configuration`. Checked against a real
    table so the scan succeeds regardless of the flag, then the connection setting is asserted.
    """
    import duckdb
    from soda.scan import Scan

    con = duckdb.connect(database=":memory:")
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1), (2), (3)) AS v(i)")

    scan = Scan()
    scan.set_scan_definition_name("test_add_duckdb_connection_honors_configuration_override")
    scan.set_data_source_name("duckdb")
    scan.add_duckdb_connection(con, configuration={"python_scan_all_frames": False})
    scan.add_sodacl_yaml_str(
        """
          checks for t:
            - row_count = 3
        """
    )
    scan.execute()

    scan.assert_no_error_logs()
    assert not scan.get_checks_fail(), scan.get_checks_fail_text()
    # Override honored: the flag was left at the user-requested value instead of being forced True.
    assert con.sql("SELECT current_setting('python_scan_all_frames')").fetchone()[0] is False


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
