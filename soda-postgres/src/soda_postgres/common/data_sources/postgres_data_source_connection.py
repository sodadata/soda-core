from __future__ import annotations

import logging
import os
import sys
import uuid
from abc import ABC
from datetime import timezone, tzinfo
from pathlib import Path
from typing import Any, Callable, ClassVar, Dict, Literal, Optional, Union

import psycopg
from pydantic import Field, IPvAnyAddress, SecretStr, field_validator
from soda_core.common.data_source_connection import (
    DataSourceConnection,
    parse_session_timezone,
)
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logging_constants import soda_logger
from soda_core.model.data_source.data_source import DataSourceBase
from soda_core.model.data_source.data_source_connection_properties import (
    DataSourceConnectionProperties,
)

logger: logging.Logger = soda_logger


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
        """Postgres override: server-side (named) cursor that streams ONE row
        at a time from the backend instead of buffering the whole result.

        The base ``DataSourceConnection.execute_query_one_by_one`` uses
        ``self.connection.cursor()`` without a name. For psycopg3 that's a
        client-side cursor: ``execute(sql)`` materialises the ENTIRE result
        set in libpq's C-side buffer before the first ``fetchone()`` returns.
        On large result sets this drove peak RSS to ~5× the source bytes
        and OOM-killed K8s pods (see memory_management/MEMORY_ANALYSIS.md).

        Strategy: open a named (server-side) cursor and `fetchone()` per
        row. Memory footprint is bounded by the size of the single largest
        row plus libpq's per-fetch frame, regardless of result-set size.
        Throughput is intentionally traded for memory predictability — the
        previous adaptive-itersize controller is retained as dead code
        below for future use, but is bypassed in this code path.

        Caveats:
          * Server-side cursors require an open transaction. If the
            connection is in autocommit mode, fall back to the base impl
            (the buffered behaviour). Default psycopg3 connections aren't
            autocommit, so this is normally fine.
          * Server-side cursors hold a snapshot on the backend — long
            transactions can interact with vacuum / replication slots.
            For Soda's typical scan duration (seconds) this is irrelevant.
        """
        if getattr(self.connection, "autocommit", False):
            # Server-side cursors can't be created in autocommit mode — fall
            # back to the buffered base implementation rather than fail.
            logger.debug(
                "execute_query_one_by_one: connection is in autocommit mode, "
                "falling back to buffered client-side cursor"
            )
            try:
                return super().execute_query_one_by_one(
                    sql, row_callback, log_query=log_query, row_limit=row_limit
                )
            except psycopg.errors.Error as e:
                logger.warning(f"SQL query one-by-one failed: \n{sql}\n{e}")
                logger.debug("Rolling back transaction")
                self.rollback()
                raise e

        cursor_name = f"soda_stream_{uuid.uuid4().hex[:12]}"
        if log_query:
            logger.debug(
                f"SQL query one-by-one (server-side cursor {cursor_name}, "
                f"fetchone per row — minimise memory):\n{sql}"
            )

        try:
            with self.connection.cursor(name=cursor_name) as cursor:
                cursor.itersize = 1
                cursor.execute(sql)
                description: tuple[tuple] = cursor.description
                rows_processed: int = 0
                while True:
                    if row_limit is not None and rows_processed >= row_limit:
                        break
                    row = cursor.fetchone()
                    if row is None:
                        break
                    row_callback(row, description)
                    rows_processed += 1
            return description
        except psycopg.errors.Error as e:
            logger.warning(f"SQL query one-by-one failed: \n{sql}\n{e}")
            logger.debug("Rolling back transaction")
            self.rollback()
            raise e

    @staticmethod
    def _resolve_fixed_fetch_size() -> Optional[int]:
        """If ``SODA_STREAMING_FETCH_SIZE`` is set to a positive int, return
        it (forces a fixed itersize, bypasses adaptive). Otherwise return
        None (adaptive mode is the default)."""
        raw = os.environ.get("SODA_STREAMING_FETCH_SIZE")
        if not raw:
            return None
        try:
            value = int(raw)
            return value if value > 0 else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _resolve_fetch_target_bytes() -> int:
        """Target byte budget per fetch batch in adaptive mode.

        Env: ``SODA_STREAMING_FETCH_TARGET_BYTES`` (positive int, bytes).
        Default 50 MB — leaves comfortable headroom under a ~500 MB K8s pod
        cap given soda's other resident state (startup floor + inserter
        accumulator + COPY-write spike at flush time).
        """
        raw = os.environ.get("SODA_STREAMING_FETCH_TARGET_BYTES")
        default = 50 * 1024 * 1024
        if not raw:
            return default
        try:
            value = int(raw)
            return value if value > 0 else default
        except (TypeError, ValueError):
            return default


class _AdaptiveFetchSizeController:
    """TCP-ramp-style adaptive ``cursor.itersize`` controller for a single
    streaming query.

    Behaviour:
      * Starts at ``MIN_SIZE`` (TCP-style slow start — cautious in case the
        first rows happen to be huge).
      * Samples row size at row positions ``[1, 10, 100, 1000]`` (rapid
        calibration), then every ``RESAMPLE_INTERVAL`` rows (steady state).
      * At each re-sample, computes ``desired = target_bytes // row_size``
        and applies bounded change: at most ``2×`` up or ``0.5×`` down per
        reaction. Bounds runaway growth on an outlier-small row AND
        oscillation when the result set's row size is variable.
      * Bounded by ``[MIN_SIZE, MAX_SIZE]`` — won't go below 1 (the smallest
        unit) or above 10000 (round-trip latency dominates beyond this).

    Cheap: row-size estimation only runs at re-sample points (not every row).
    """

    MIN_SIZE: int = 1
    MAX_SIZE: int = 10_000
    RESAMPLE_INTERVAL: int = 1000  # in steady state
    # Slow-start schedule (orders of magnitude) — three re-samples in the
    # first 1000 rows, where uncertainty about row size is highest.
    _RESAMPLE_LADDER: tuple[int, ...] = (1, 10, 100, 1000)

    def __init__(self, target_bytes: int) -> None:
        self.target_bytes = target_bytes
        self.current = self.MIN_SIZE
        self.rows_seen = 0
        self._ladder_idx = 0

    def observe_and_maybe_adjust(self, sample_row: Any) -> Optional[int]:
        """Called once per batch with the LAST row of that batch (cheap to
        estimate; representative enough for the controller's purposes).
        Updates ``self.current`` if the controller decides to grow/shrink.

        Returns the new batch size if it changed, else None. The caller is
        responsible for using ``self.current`` for the next ``fetchmany()``
        call — this method does NOT mutate any cursor state.

        Row-size estimation is only performed at re-sample points — for
        batches in between this method is essentially a counter increment.
        """
        # We can't count individual rows here (we only see batch ends), but
        # for re-sampling cadence what matters is "how much have we
        # processed so far" — approximate by treating each call as one
        # 'progress tick' = current batch size.
        self.rows_seen += self.current
        if not self._is_resample_point():
            return None

        row_bytes = self._estimate_row_size(sample_row)
        if row_bytes <= 0:
            return None

        desired = max(self.MIN_SIZE, min(self.MAX_SIZE, self.target_bytes // row_bytes))

        # Rate limit: at most 2× up or 0.5× down per reaction.
        if desired > self.current * 2:
            new_size = self.current * 2
        elif self.current > 1 and desired < self.current // 2:
            new_size = max(self.MIN_SIZE, self.current // 2)
        else:
            new_size = desired
        new_size = max(self.MIN_SIZE, min(self.MAX_SIZE, new_size))

        if new_size == self.current:
            return None
        self.current = new_size
        return new_size

    def _is_resample_point(self) -> bool:
        if self._ladder_idx < len(self._RESAMPLE_LADDER):
            if self.rows_seen >= self._RESAMPLE_LADDER[self._ladder_idx]:
                self._ladder_idx += 1
                return True
            return False
        # Steady state: every RESAMPLE_INTERVAL rows after the ladder ends.
        return (self.rows_seen - self._RESAMPLE_LADDER[-1]) % self.RESAMPLE_INTERVAL == 0

    @staticmethod
    def _estimate_row_size(row: Any) -> int:
        """Rough Python-side estimate of a row's resident size in bytes.

        Sums ``sys.getsizeof`` on the tuple container and each value. ``None``
        values are estimated at 16 bytes (a Python pointer). Applies a 1.5×
        safety factor because ``sys.getsizeof`` underestimates for nested
        container types and doesn't include the libpq C-side decoded copy.
        """
        try:
            total = sys.getsizeof(row)
            for v in row:
                total += sys.getsizeof(v) if v is not None else 16
            return int(total * 1.5)
        except Exception:  # noqa: BLE001 — best-effort estimator, never raise
            return 0
