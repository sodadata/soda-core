from textwrap import dedent

from helpers.fixtures import *  # NOQA
from contracts.helpers.contract_fixtures import *  # NOQA

from soda.contracts.connection import Connection


@pytest.fixture(scope="session")
def connection():
    with Connection.from_yaml_str(dedent("""
        type: postgres
        host: localhost
        database: sodasql
        username: sodasql
        password: ${POSTGRES_PWD}
    """)) as connection:
        yield connection
