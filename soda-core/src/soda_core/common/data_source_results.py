from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from itertools import islice
from typing import Any, Optional, Protocol


class Cursor(Protocol):
    """DB-API 2.0 Cursor protocol.

    This is a subset of the DB-API 2.0 Cursor protocol defined in PEP-0249.
    """

    @property
    def description(self) -> Sequence[tuple[str, Any, Any, Any, Any, Any, Any]]:
        """Gets the description of the columns in the result set.

        Each column is represented as a tuple containing:
            - name
            - type_code
            - display_size
            - internal_size
            - precision
            - scale
            - null_ok

        The first two items (name and type_code) are mandatory, the other five are optional and are set to None if no
        meaningful values can be provided.
        """

    @property
    def rowcount(self) -> int:
        """Gets the number of rows that the last execute*() produced (for DQL statements like SELECT) or affected (for
        DML statements like UPDATE or INSERT).
        """

    def fetchone(self) -> Sequence[Any] | None:
        """Fetches the next row of a query result set, returning a single sequence, or None when no more data is
        available.
        """

    def fetchmany(self, size: int) -> Sequence[Sequence[Any]]:
        """Fetches the next set of rows of a query result, returning a list of sequences. An empty list is returned when
        no more rows are available.
        """

    def fetchall(self) -> Sequence[Sequence[Any]]:
        """Fetches all (remaining) rows of a query result, returning them as a list of sequences."""

    def close(self) -> None:
        """Closes the cursor."""


class QueryResult:
    def __init__(self, rows: list[tuple], columns: tuple[tuple]):
        self.rows: list[tuple] = rows
        self.columns: tuple[tuple] = columns


class QueryResultIterator:
    """The rows of one query, read once, in order.

    By default the rows come from the cursor's ``fetchone``. A connection that fetches another way passes its own
    ``rows``, such as a server-side cursor or one result batch at a time. The cursor still gives the column metadata
    and the row count either way.
    """

    def __init__(
        self,
        cursor: Cursor,
        format_row: Callable[[Any], tuple],
        rows: Optional[Iterator[Sequence[Any]]] = None,
    ):
        self._cursor = cursor
        self._format_row = format_row
        self._rows: Iterator[Sequence[Any]] = iter(cursor.fetchone, None) if rows is None else rows

    def __iter__(self) -> QueryResultIterator:
        return self

    def __next__(self) -> Sequence[Any]:
        return self._format_row(next(self._rows))

    def batches(self, max_rows: int) -> Iterator[list[Sequence[Any]]]:
        """Reads the rest of the rows in lists of at most ``max_rows``.

        This reads the same rows as iterating, so it does not run the query again. ``max_rows`` sets only the size of
        the lists handed back. How many rows the driver fetches from the database at a time is up to the connection.
        """
        if max_rows < 1:
            raise ValueError(f"max_rows must be at least 1, not {max_rows}")
        return iter(lambda: list(islice(self, max_rows)), [])

    @property
    def row_count(self) -> int:
        """Returns the total number of rows in the result set.

        Note this field is not reliable on all datasources. DuckDB for example returns -1, and Oracle returns the number
        of rows fetched so far.
        """
        return self._cursor.rowcount

    @property
    def columns(self) -> dict[str, Any]:
        """Returns the columns metadata as a dictionary mapping column names to type codes."""
        return {column[0]: column[1] for column in self._cursor.description}
