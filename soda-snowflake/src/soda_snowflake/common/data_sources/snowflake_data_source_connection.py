from __future__ import annotations

import logging
from abc import ABC
from datetime import timezone, tzinfo
from pathlib import Path
from typing import Callable, Dict, Iterator, Literal, Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pydantic import Field, SecretStr, field_validator
from snowflake import connector
from snowflake.connector.errors import OperationalError
from soda_core.common.data_source_connection import (
    DataSourceConnection,
    memory_optimized_driver_settings,
    parse_session_timezone,
)
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import DataSourceConnectionProperties

try:
    from snowflake.connector.errorcode import ER_INCOMPLETE_RESULT_CHUNK
except ImportError:  # connectors before 4.8.0 do not define it
    ER_INCOMPLETE_RESULT_CHUNK = 252013

logger: logging.Logger = soda_logger

USER_DESCRIPTION = "Username for authentication"


class SnowflakeConnectionProperties(DataSourceConnectionProperties, ABC):
    ...


class SnowflakeSharedConnectionProperties(SnowflakeConnectionProperties, ABC):
    account: str = Field(..., description="Snowflake account identifier")
    warehouse: Optional[str] = Field(None, description="Name of the warehouse to use")
    database: Optional[str] = Field(None, description="Name of the database to use")
    role: Optional[str] = Field(None, description="Role to assume after connecting")
    session_parameters: Optional[Dict[str, str]] = Field(None, description="Session-level parameters")
    host: Optional[str] = Field(None, description="Host name of the Snowflake account")


class SnowflakePasswordAuth(SnowflakeSharedConnectionProperties):
    user: str = Field(..., description=USER_DESCRIPTION)
    password: SecretStr = Field(..., description="User password")


class SnowflakeKeyPairAuth(SnowflakeSharedConnectionProperties):
    user: str = Field(..., description=USER_DESCRIPTION)
    private_key: SecretStr = Field(..., description="Private key for authentication")
    private_key_passphrase: Optional[SecretStr] = Field(None, description="Passphrase if private key is encrypted")

    def to_connection_kwargs(self) -> dict:
        connection_kwargs = super().to_connection_kwargs()
        connection_kwargs["private_key"] = self._decrypt(self.private_key, self.private_key_passphrase)
        # Remove the passphrase from the connection kwargs, it's already included in the `private_key` field.
        connection_kwargs.pop("private_key_passphrase", None)
        return connection_kwargs

    def _decrypt(self, private_key: str, private_key_passphrase: Optional[SecretStr]) -> bytes:
        private_key_bytes = private_key.get_secret_value().encode()
        private_key_passphrase_bytes = (
            private_key_passphrase.get_secret_value().encode() if private_key_passphrase else None
        )

        p_key = serialization.load_pem_private_key(
            private_key_bytes,
            password=private_key_passphrase_bytes,
            backend=default_backend(),
        )

        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )


class SnowflakeKeyPairFileAuth(SnowflakeSharedConnectionProperties):
    user: str = Field(..., description=USER_DESCRIPTION)
    private_key_path: Path = Field(..., description="Path to private key file")
    private_key_passphrase: Optional[SecretStr] = Field(None, description="Passphrase if private key is encrypted")

    def to_connection_kwargs(self) -> dict:
        connection_kwargs = super().to_connection_kwargs()
        # Snowflake does not need this extra info. It will read the private key file directly.
        connection_kwargs.pop("private_key_path", None)
        connection_kwargs.pop("private_key_passphrase", None)
        connection_kwargs["private_key_file"] = self.private_key_path
        if self.private_key_passphrase is not None:
            pwd = self.private_key_passphrase.get_secret_value()
            if pwd:
                connection_kwargs["private_key_file_pwd"] = pwd.encode()
        return connection_kwargs


class SnowflakeOAuthAuth(SnowflakeSharedConnectionProperties):
    authenticator: Literal["oauth"] = Field(..., description="Use OAuth access token")
    token: SecretStr = Field(..., description="OAuth access token")


class SnowflakeClientCredentialsOAuthAuth(SnowflakeSharedConnectionProperties):
    # User is not required for OAuth Client Credentials Flow.
    authenticator: Literal["OAUTH_CLIENT_CREDENTIALS"] = Field(
        ..., description="Authenticator to use for OAuth Client Credentials Flow."
    )
    oauth_client_id: SecretStr = Field(..., description="Client ID for OAuth Client Credentials Flow.")
    oauth_client_secret: SecretStr = Field(..., description="Client secret for OAuth Client Credentials Flow.")
    oauth_token_request_url: SecretStr = Field(..., description="Token request URL for OAuth Client Credentials Flow.")
    oauth_scope: Optional[SecretStr] = Field(None, description="Scope for OAuth Client Credentials Flow if required.")


class SnowflakeSSOAuth(SnowflakeSharedConnectionProperties):
    # User is not required for SSO Flow.
    authenticator: Literal["externalbrowser"] = Field("externalbrowser", description="Use external browser SSO login")


class SnowflakeDataSource(DataSourceBase, ABC):
    type: Literal["snowflake"] = Field("snowflake")
    connection_properties: SnowflakeConnectionProperties = Field(
        ..., alias="connection", description="Snowflake connection configuration"
    )

    @field_validator("connection_properties", mode="before")
    @classmethod
    def infer_connection_type(cls, value):
        if "password" in value:
            return SnowflakePasswordAuth(**value)
        elif "private_key" in value:
            return SnowflakeKeyPairAuth(**value)
        elif "private_key_path" in value:
            return SnowflakeKeyPairFileAuth(**value)
        elif "token" in value:
            return SnowflakeOAuthAuth(**value)
        elif value.get("authenticator") == "externalbrowser":
            return SnowflakeSSOAuth(**value)
        elif value.get("authenticator") == "OAUTH_CLIENT_CREDENTIALS":
            return SnowflakeClientCredentialsOAuthAuth(**value)
        raise ValueError("Could not infer Snowflake connection type from input")


class SnowflakeDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: SnowflakeConnectionProperties,
    ):
        return connector.connect(
            application="Soda",
            **config.to_connection_kwargs(),
        )

    def _fetch_session_timezone(self) -> tzinfo:
        with self.connection.cursor() as cursor:
            cursor.execute("SHOW PARAMETERS LIKE 'TIMEZONE' IN SESSION")
            rows = cursor.fetchall()
            description = cursor.description
        if not rows:
            # SHOW PARAMETERS returned no row at all — treat as "no session TZ
            # configured" rather than a parse failure (parse_session_timezone returns
            # UTC silently for empty input but raises on truly junk values).
            return timezone.utc
        column_names = [col[0].lower() if col[0] else "" for col in description]
        try:
            value_index = column_names.index("value")
        except ValueError:
            # Defensive: a Snowflake driver / server upgrade has reshaped the SHOW
            # PARAMETERS output. We refuse to guess a column index — the prior code
            # silently fell back to ``column 1`` which would happen to be right today
            # but would mask a real schema change tomorrow. Surface the situation as a
            # warning and default to UTC; the connection-level wrapper will not log it
            # again because we return rather than raise.
            logger.warning(
                "Snowflake SHOW PARAMETERS did not include a 'value' column "
                f"(got {column_names!r}); defaulting session timezone to UTC"
            )
            return timezone.utc
        return parse_session_timezone(rows[0][value_index])

    def supports_streaming_fetch(self) -> bool:
        return True

    def _execute_query_one_by_one_streaming(
        self,
        sql: str,
        row_callback: Callable[[tuple, tuple[tuple]], None],
        log_query: bool = True,
        row_limit: Optional[int] = None,
    ) -> tuple[tuple]:
        """Reads the result one result batch at a time, without read-ahead.

        The cursor's own fetch downloads several batches ahead of the reader
        (``client_prefetch_threads``, 4 by default), and recent connectors turn each
        downloaded batch into a full list of Python rows. On a large result that
        read-ahead alone takes hundreds of MB. Here the next batch is downloaded only
        once the previous one is used up, so memory stays at about one batch. Each batch
        goes through the same ``create_iter`` call the cursor's fetch makes, so the rows
        have the same Python types as on the buffered path.
        """
        memory_optimized_driver_settings.log_active_once(type(self).__name__)
        cursor = self.connection.cursor()
        try:
            if log_query:
                logger.debug(f"SQL query one-by-one (one result batch at a time, no read-ahead):\n{sql}")
            cursor.execute(sql)
            description: tuple[tuple] = cursor.description
            rows: Iterator[tuple] = self._iterate_rows_one_batch_at_a_time(cursor)
            rows_processed: int = 0
            while row_limit is None or rows_processed < row_limit:
                row = next(rows, None)
                if row is None:
                    self._raise_if_result_cut_short(cursor, rows_processed)
                    break
                rows_processed += 1
                row_callback(row, description)
        finally:
            cursor.close()
        return description

    def _iterate_rows_one_batch_at_a_time(self, cursor) -> Iterator[tuple]:
        # get_result_batches() returns None when the query left no result set.
        for result_batch in cursor.get_result_batches() or []:
            for row in result_batch.create_iter(connection=self.connection):
                # The connector hands conversion errors over as rows; its own fetchone() raises them.
                if isinstance(row, Exception):
                    raise row
                yield row

    @staticmethod
    def _raise_if_result_cut_short(cursor, rows_processed: int) -> None:
        # Walking the batches skips the end-of-result check that the cursor's own fetch
        # makes, so make it here: a result that ends before the row count Snowflake
        # reported must fail, not pass on a partial list.
        if cursor.rowcount is not None and rows_processed < cursor.rowcount:
            raise OperationalError(
                msg=f"Incomplete result: the result set produced {rows_processed} row(s) "
                f"but the server reported {cursor.rowcount}.",
                errno=ER_INCOMPLETE_RESULT_CHUNK,
            )
