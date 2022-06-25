from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List


@dataclass
class TablePrinter:
    def __init__(
        self,
        cursor: object | None = None,
        rows: Iterable[Iterable] | None = None,
        column_names: Iterable[str] | None = None,
    ):
        """
        :param cursor: DBAPI cursor. Mutually exclusive with rows and column_names
        :param rows: Row data. Mutually exclusive with cursor
        :param column_names: Column names. Mutually exclusive with cursor
        :except ValueError when cursor or rows and column_names are not supplied mutually exclusive.
        """
        self.rows: Iterable[Iterable] = self._get_rows(cursor, rows)
        self.column_names: Iterable[str] = self._get_column_names(cursor, column_names)

    def _get_rows(self, cursor, rows) -> Iterable[Iterable]:
        if cursor is None and rows is None:
            raise ValueError("Either cursor or rows have to be provided. Both were None.")
        if cursor is not None and rows is not None:
            raise ValueError("Either cursor or rows have to be provided. Both were not None.")
        if rows is not None:
            return rows
        return cursor.fetchall()

    def _get_column_names(self, cursor, column_names) -> Iterable[str]:
        if cursor is None and column_names is None:
            raise ValueError("Either cursor or column_names have to be provided. Both were None.")
        if cursor is not None and column_names is not None:
            raise ValueError("Either cursor or column_names have to be provided. Both were not None.")
        if column_names:
            return column_names
        return self.get_column_names_from_cursor_description(cursor)

    @staticmethod
    def get_column_names_from_cursor_description(cursor):
        return [column[0] for column in cursor.description]

    def get_lines(self, max_column_length: int = 25, quote_strings: bool = False) -> list[str]:
        column_titles = self._serialize_row(self.column_names, max_column_length=max_column_length, quote_strings=False)

        lengths = []
        rules = []
        rows = [
            self._serialize_row(row, max_column_length=max_column_length, quote_strings=quote_strings)
            for row in self.rows
        ]

        for column_title in column_titles:
            lengths.append(len(column_title))
        for column_index in range(len(lengths)):
            rls = [len(row[column_index]) for row in rows if row[column_index]]
            lengths[column_index] = max([lengths[column_index]] + rls)
            rules.append("-" * lengths[column_index])
        format = "│".join(["%%-%ss" % length for length in lengths])
        format = f"│{format}│"
        result = [format % tuple(column_titles)]
        result.append(format % tuple(rules))
        for row in rows:
            result.append(format % tuple(row))
        return result

    def get_table(self, max_column_length: int = 25, prefix: str = "") -> str:
        return prefix + f"\n{prefix}".join(self.get_lines(max_column_length=max_column_length))

    def _serialize_row(self, row, max_column_length: int, quote_strings: bool):
        return [self._maxify(self._stringify(value, quote_strings), max_column_length) for value in row]

    def _stringify(self, value, quote_strings: bool):
        if isinstance(value, str):
            return f"'{value}'" if quote_strings else value
        if value is None:
            return ""
        return str(value)

    def _maxify(self, value, max_column_length: int):
        return value if len(value) <= max_column_length else f"{value[:max_column_length - 3]}..."
