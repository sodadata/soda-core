import pytest


@pytest.fixture
def arch_sql_dialect():
    """Skips a dialect rule when no collection hook supplied the dialects, so a plain `pytest` run does not error."""
    pytest.skip("needs a collection hook that supplies the dialects, e.g. -p helpers.sql_dialect_arch_plugin")
