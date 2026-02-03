from collections.abc import Sequence
from typing import Any, Protocol


class Cursor(Protocol):
    """DB-API 2.0 Cursor protocol.

    This is a subset of the DB-API 2.0 Cursor protocol defined in PEP-0249.
    """

    @property
    def description(self) -> Sequence[tuple[str, str, Any, Any, Any, Any, Any]]:
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
    def __init__(self, cursor: Cursor):
        self._cursor = cursor
        self._position = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._cursor.close()

    def __iter__(self):
        return self

    def __next__(self):
        row = self._cursor.fetchone()
        if row is None:
            self._cursor.close()
            raise StopIteration
        self._position += 1
        return row

    @property
    def row_count(self) -> int:
        """Returns the total number of rows in the result set."""
        return self._cursor.rowcount

    @property
    def position(self) -> int:
        """Returns the current position in the result set."""
        return self._position

    @property
    def columns(self) -> dict[str, str]:
        """Returns the columns metadata as a dictionary mapping column names to type codes."""
        return {column[0]: column[1] for column in self._cursor.description}


class UpdateResult:
    def __init__(
        self,
        result: object,
    ):
        self.results: object = result
