from __future__ import annotations

import logging

from pydantic import SecretStr
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SqlServerDataSourceConnection,
    SqlServerPasswordAuth,
)


def test_connection_parameter_logs_redact_values(caplog) -> None:
    instance = SqlServerDataSourceConnection.__new__(SqlServerDataSourceConnection)
    config = SqlServerPasswordAuth(
        host="localhost",
        port=1433,
        database="master",
        user="sa",
        password=SecretStr("Password1!"),
        connection_parameters={
            "application_intent": "ReadOnly",
            "access_token": "super-secret-token",
        },
    )

    with caplog.at_level(logging.INFO):
        connection_string = instance.build_connection_string(config)

    assert "application_intent=ReadOnly" in connection_string
    assert "access_token=super-secret-token" in connection_string

    assert "Adding connection parameter: application_intent=<redacted>" in caplog.text
    assert "Adding connection parameter: access_token=<redacted>" in caplog.text
    assert "ReadOnly" not in caplog.text
    assert "super-secret-token" not in caplog.text
