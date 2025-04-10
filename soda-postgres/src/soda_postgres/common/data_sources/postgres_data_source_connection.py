from __future__ import annotations

from pathlib import Path
from typing import Union

import psycopg2
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)
from soda_postgres.model.data_source.postgres_connection_properties import (
    PostgresConnectionPassword,
    PostgresConnectionPasswordFile,
    PostgresConnectionString,
)


class PostgresDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: Union[PostgresConnectionString, PostgresConnectionPassword, PostgresConnectionPasswordFile],
    ):
        if isinstance(config, PostgresConnectionString):
            if config.connection_string:
                return psycopg2.connect(config.connection_string)
            else:
                raise ValueError("PostgresConnectionString must have `connection_string`")

        conn_kwargs = {
            "host": str(config.host),
            "port": config.port,
            "dbname": config.database,
            "user": config.user,
            "sslmode": config.ssl_mode,
        }

        # SSL certs
        if config.ssl_cert:
            conn_kwargs["sslcert"] = config.ssl_cert
        if config.ssl_key:
            conn_kwargs["sslkey"] = config.ssl_key
        if config.ssl_root_cert:
            conn_kwargs["sslrootcert"] = config.ssl_root_cert

        # Password
        if isinstance(config, PostgresConnectionPassword):
            conn_kwargs["password"] = config.password.get_secret_value()
        elif isinstance(config, PostgresConnectionPasswordFile):
            conn_kwargs["password"] = Path(config.password_file).read_text().strip()

        # Timeout
        if config.connection_timeout:
            conn_kwargs["connect_timeout"] = config.connection_timeout

        return psycopg2.connect(**conn_kwargs)
