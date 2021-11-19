import pytest

from sodasql import dbt as soda_dbt


def test_parse_run_results_raises_not_implemented_error() -> None:
    """
    A NotImplementedError should be raised when run results version is not v3.
    """
    with pytest.raises(NotImplementedError):
        soda_dbt.parse_run_results({"metadata": {"dbt_schema_version": "not v3"}})
