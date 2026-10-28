import pytest
from soda.data_sources.spark_df_connection import SparkDfConnection


class _DataFrameWithoutOffset:
    """Like a classic pyspark < 3.5 DataFrame."""


class _DataFrame:
    def offset(self, num):
        return self


class _Session:
    def __init__(self, df):
        self._df = df

    def range(self, start):
        return self._df


def test_connection_without_session_names_add_spark_session():
    with pytest.raises(ValueError, match="add_spark_session"):
        SparkDfConnection(None)


def test_connection_rejects_dataframes_without_offset():
    with pytest.raises(RuntimeError, match=r"soda-core-spark-df\[pyspark\]"):
        SparkDfConnection(_Session(_DataFrameWithoutOffset()))


def test_connection_accepts_dataframes_with_offset():
    session = _Session(_DataFrame())
    assert SparkDfConnection(session).cursor().spark_session is session
