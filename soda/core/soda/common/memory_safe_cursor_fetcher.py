from typing import List, Tuple

from soda.common.logs import Logs

BATCH_SIZE = 100


class MemorySafeCursorFetcher:
    def __init__(self, cursor, limit=10000):
        self._cursor = cursor
        self._logs = Logs()
        self.limit = limit
        self.rows = None
        self.limit_exhausted = False
        self.total_row_count = -1

    def get_row_count(self) -> int:
        self.get_rows()
        return self.total_row_count

    def get_rows(self) -> List[Tuple]:
        if self.rows is not None:
            return self.rows

        self.rows = []
        self.total_row_count = 0
        while True:
            results = self._cursor.fetchmany(BATCH_SIZE)
            # Make sure to empty th entire [remote] cursor, even if results are
            # no longer needed.
            if not results or len(results) == 0:
                break

            # Count all rows, regardless of storing
            self.total_row_count += len(results)

            # Only store the needed number of results in memory
            if len(self.rows) < self.limit:
                self.rows.extend(results[: self.limit - len(self.rows)])
            elif self.limit_exhausted is False:
                self._logs.warning(
                    "The query produced a lot of results, which have not all been stored in memory. "
                    f"Soda limits the number of processed results for sampling-like use-cases to {self.limit}. "
                    "You might want to consider optimising your query to select less results."
                )
                self.limit_exhausted = True

        return self.rows
