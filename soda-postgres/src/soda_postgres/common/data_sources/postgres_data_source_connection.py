from __future__ import annotations

import logging
import uuid
from abc import ABC
from datetime import timezone, tzinfo
from pathlib import Path
from typing import Callable, ClassVar, Dict, Literal, Optional, Union

import psycopg
from pydantic import Field, IPvAnyAddress, SecretStr, field_validator
from soda_core.common.data_source_connection import (
    DataSourceConnection,
    parse_session_timezone,
)
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logging_constants import soda_logger
from soda_core.common.value_size import estimate_value_size
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger: logging.Logger = soda_logger

# Client-side buffer budget for the streaming (named-cursor) fetch path.
# Batch size adapts to the widest row seen so far: rows at or above the
# budget fetch one at a time; thin rows amortise FETCH round-trips up to
# the row cap instead of paying one round-trip per row.
STREAM_FETCH_BUDGET_BYTES: int = 8 * 1024 * 1024
STREAM_FETCH_MAX_BATCH_ROWS: int = 1000
# Batches may grow at most 4x per fetch. Without the ramp, one thin probe
# row (NULL-heavy, no ORDER BY correlation) jumps the batch straight to the
# row cap before any fat row has been seen — 1000 x 0.5 MB rows is a 500 MB
# fetchmany. With it, overshoot is bounded by ~4x the previous batch.
STREAM_FETCH_GROWTH_FACTOR: int = 4


def _estimate_row_bytes(row: tuple) -> int:
    # estimate_value_size recurses into container values (psycopg3 returns
    # jsonb as parsed dicts/lists) — shallow getsizeof counted a 10 MB json
    # document as ~hundreds of bytes, pinning the batch at the row cap.
    return sum(estimate_value_size(value) for value in row)


class PostgresConnectionProperties(DataSourceConnectionProperties, ABC):
    field_mapping: ClassVar[Dict[str, str]] = {
        "database": "dbname",
    }


class PostgresConnectionString(PostgresConnectionProperties):
    connection_string: str = Field(..., description="Complete connection string (alternative to individual parameters)")


class PostgresConnectionPropertiesBase(PostgresConnectionProperties, ABC):
    host: Union[str, IPvAnyAddress] = Field(..., description="Database host (hostname or IP address)")
    port: int = Field(5432, description="Database port (1-65535)", ge=1, le=65535)
    database: str = Field(..., description="Database name", min_length=1, max_length=63)
    user: str = Field(..., description="Database user (1-63 characters)", min_length=1, max_length=63)

    # SSL configuration
    sslmode: Literal["disable", "allow", "prefer", "require", "verify-ca", "verify-full"] = Field(
        "prefer", description="SSL mode for the connection"
    )
    sslcert: Optional[str] = Field(None, description="Path to SSL client certificate")
    sslkey: Optional[str] = Field(None, description="Path to SSL client key")
    sslrootcert: Optional[str] = Field(None, description="Path to SSL root certificate")

    # Connection options
    connection_timeout: Optional[int] = Field(None, description="Connection timeout in seconds")


class PostgresConnectionPassword(PostgresConnectionPropertiesBase):
    password: SecretStr = Field(..., description="Database password")


class PostgresConnectionPasswordFile(PostgresConnectionPropertiesBase):
    password_file: Path = Field(..., description="Path to file containing database password")


class PostgresDataSource(DataSourceBase, ABC):
    type: Literal["postgres"] = Field("postgres")
    connection_properties: PostgresConnectionProperties = Field(
        ..., alias="connection", description="Data source connection details"
    )

    @field_validator("connection_properties", mode="before")
    def infer_connection_type(cls, value):
        if "password" in value:
            return PostgresConnectionPassword(**value)
        elif "password_file" in value:
            return PostgresConnectionPasswordFile(**value)
        raise ValueError("Unknown connection structure")


class PostgresDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: DataSourceConnectionProperties):
        super().__init__(name, connection_properties)

    def _create_connection(
        self,
        config: PostgresConnectionProperties,
    ):
        if isinstance(config, PostgresConnectionPasswordFile):
            with open(config.password_file, "r") as f:
                config_dict = config.model_dump(exclude="password_file")
                config_dict["password"] = f.read().strip()
                config = PostgresConnectionPassword(**config_dict)
        connection_kwargs = config.to_connection_kwargs()
        connection = psycopg.connect(**connection_kwargs)
        return connection

    def _fetch_session_timezone(self) -> tzinfo:
        with self.connection.cursor() as cursor:
            cursor.execute("SHOW timezone")
            row = cursor.fetchone()
        if not row:
            return timezone.utc
        return parse_session_timezone(row[0])

    def execute_query(self, sql: str, log_query: bool = True) -> QueryResult:
        try:
            return super().execute_query(sql, log_query=log_query)
        except psycopg.errors.Error as e:  # Catch the error and roll back the transaction
            logger.warning(f"SQL query failed: \n{sql}\n{e}")
            logger.debug("Rolling back transaction")
            self.rollback()
            raise e

    def execute_update(self, sql: str, log_query: bool = True) -> int:
        try:
            return super().execute_update(sql, log_query=log_query)
        except psycopg.errors.Error as e:  # Catch the error and roll back the transaction
            logger.warning(f"SQL update failed: \n{sql}\n{e}")
            logger.debug("Rolling back transaction")
            self.rollback()
            raise e

    def execute_query_one_by_one(
        self,
        sql: str,
        row_callback: Callable[[tuple, tuple[tuple]], None],
        log_query: bool = True,
        row_limit: Optional[int] = None,
    ) -> tuple[tuple]:
        # Pre-memory-work behavior, restored: the buffered base
        # implementation with postgres's rollback-on-error wrapper. Callers
        # that stream large result sets use
        # execute_query_one_by_one_memory_optimized instead.
        try:
            return super().execute_query_one_by_one(sql, row_callback, log_query=log_query, row_limit=row_limit)
        except psycopg.errors.Error as e:  # Catch the error and roll back the transaction
            logger.warning(f"SQL query one-by-one failed: \n{sql}\n{e}")
            logger.debug("Rolling back transaction")
            self.rollback()
            raise e

    def execute_query_one_by_one_memory_optimized(
        self,
        sql: str,
        row_callback: Callable[[tuple, tuple[tuple]], None],
        log_query: bool = True,
        row_limit: Optional[int] = None,
    ) -> tuple[tuple]:
        """Postgres override: server-side (named) cursor that streams rows
        in byte-budgeted batches instead of buffering the whole result.

        The base ``DataSourceConnection.execute_query_one_by_one`` uses
        ``self.connection.cursor()`` without a name. For psycopg3 that's a
        client-side cursor: ``execute(sql)`` materialises the ENTIRE result
        set in libpq's C-side buffer before the first ``fetchone()`` returns.
        On large result sets this drove peak RSS to ~5× the source bytes
        and OOM-killed K8s pods (see memory_management/MEMORY_ANALYSIS.md).

        Strategy: open a named (server-side) cursor and fetch in adaptive
        batches. The first fetch is a single probe row; after that the
        batch size is ``STREAM_FETCH_BUDGET_BYTES`` divided by the widest
        row observed so far, clamped to [1, STREAM_FETCH_MAX_BATCH_ROWS].
        Fat rows (>= budget) therefore stream one at a time, while thin
        rows amortise FETCH round-trips. Memory footprint is bounded by
        roughly one batch (~the budget) plus the single largest row,
        regardless of result-set size.

        Caveats:
          * Server-side cursors require an open transaction. If the
            connection is in autocommit mode, fall back to the base impl
            (the buffered behaviour). Default psycopg3 connections aren't
            autocommit, so this is normally fine.
          * Server-side cursors hold a snapshot on the backend — long
            transactions can interact with vacuum / replication slots.
            For Soda's typical scan duration (seconds) this is irrelevant.
          * ``withhold=True`` keeps the cursor alive past intermediate
            ``connection.commit()`` calls — needed because the
            failed-rows DWH pump path commits at the end of every
            ``_optimized_insert`` batch on the SAME connection. Without
            this, a flush triggered mid-iteration kills the source cursor
            and the next ``fetchone()`` raises ``cursor "soda_stream_…"
            does not exist``. Postgres materialises the cursor's
            remaining unfetched rows server-side at commit time; the
            client-side memory footprint stays bounded.
          * KNOWN LIMITATION: this cursor is only safe from a writer
            ROLLBACK once it has been "held" — postgres materialises a
            ``withhold=True`` cursor at the first ``commit()`` on its
            connection. Before that first commit, a rollback on the same
            connection destroys the not-yet-held cursor (``InvalidCursorName``
            on the next fetch). The between-source DWH pump shares this
            connection with its bulk-insert writer (postgres uses the base
            ``get_connection_for_dwh``, which returns the same connection —
            only SqlServer overrides it to a separate one). In practice the
            window is narrow: the FIRST ``_optimized_insert`` flush commits
            (line ``connection.commit()``) and holds the cursor, so every
            later flush's failure/rollback is safe. The only exposed case is
            a COPY that fails at *setup* on the very first flush — before any
            commit — after which ``do_bulk_insert`` rolls back to retry via
            AST and trips this cursor. That surfaces as a FAILED diagnostics
            stage (the pump caller records the error), never silent row loss
            or a dangling stage. reuse_data_source forces in-source SQL
            transfer (no Python stream at all), so it is unaffected.
        """
        if getattr(self.connection, "autocommit", False):
            # Server-side cursors can't be created in autocommit mode — fall
            # back to the buffered base implementation rather than fail.
            # self.execute_query_one_by_one is the restored postgres wrapper,
            # so the rollback-on-error semantics are preserved.
            logger.debug(
                "execute_query_one_by_one_memory_optimized: connection is in autocommit mode, "
                "falling back to buffered client-side cursor"
            )
            return self.execute_query_one_by_one(sql, row_callback, log_query=log_query, row_limit=row_limit)

        cursor_name = f"soda_stream_{uuid.uuid4().hex[:12]}"
        if log_query:
            logger.debug(
                f"SQL query one-by-one (server-side cursor {cursor_name}, "
                f"byte-budgeted fetchmany — minimise memory):\n{sql}"
            )

        try:
            with self.connection.cursor(name=cursor_name, withhold=True) as cursor:
                cursor.execute(sql)
                description: tuple[tuple] = cursor.description
                rows_processed: int = 0
                batch_size: int = 1  # single probe row first, to size subsequent batches
                max_row_bytes: int = 1
                while True:
                    if row_limit is not None and rows_processed >= row_limit:
                        break
                    fetch_count: int = batch_size
                    if row_limit is not None:
                        fetch_count = min(fetch_count, row_limit - rows_processed)
                    rows = cursor.fetchmany(fetch_count)
                    if not rows:
                        break
                    for row in rows:
                        row_callback(row, description)
                        rows_processed += 1
                    # Size against the widest row seen so far (monotonic), so a
                    # late fat row permanently shrinks the batch back down.
                    # Growth is ramped: shrinking applies immediately, but the
                    # batch may only grow STREAM_FETCH_GROWTH_FACTOR x per
                    # fetch (thin-probe overshoot bound).
                    max_row_bytes = max(max_row_bytes, max(_estimate_row_bytes(row) for row in rows))
                    batch_size = max(
                        1,
                        min(
                            STREAM_FETCH_MAX_BATCH_ROWS,
                            STREAM_FETCH_BUDGET_BYTES // max_row_bytes,
                            batch_size * STREAM_FETCH_GROWTH_FACTOR,
                        ),
                    )
            return description
        except psycopg.errors.Error as e:
            # The error may come from the stream itself OR from a row_callback
            # that hit the database (the DWH pump writes from inside the
            # callback) — don't blame the SELECT. And honor log_query=False:
            # callers suppress it because the query can embed MB-scale VALUES.
            sql_blurb = f"\n{sql}" if log_query else " (query suppressed, log_query=False)"
            logger.warning(f"Streaming query (or its row callback) failed:{sql_blurb}\n{e}")
            logger.debug("Rolling back transaction")
            self.rollback()
            # Best-effort CLOSE of the named cursor. When the failure left
            # the transaction in error state, psycopg's ServerCursor.close()
            # (in the `with` exit above) skips issuing CLOSE — and a held
            # (post-commit) portal survives the rollback, materialized
            # server-side until the connection closes, which for DWH
            # connections is the whole scan. After the rollback the
            # transaction is clean, so an explicit CLOSE works; if the
            # portal is already gone (never held), it errors harmlessly.
            try:
                with self.connection.cursor() as close_cursor:
                    close_cursor.execute(f'CLOSE "{cursor_name}"')
                self.connection.commit()
            except psycopg.errors.Error as close_error:
                logger.debug(f"Best-effort CLOSE of held cursor {cursor_name} failed (harmless): {close_error}")
                try:
                    self.rollback()
                except psycopg.errors.Error:
                    pass
            raise e
