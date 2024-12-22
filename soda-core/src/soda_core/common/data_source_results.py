class QueryResult:
    def __init__(
            self,
            rows: list[tuple],
            columns: tuple[tuple]
    ):
        self.rows: list[tuple] = rows
        self.columns: tuple[tuple] = columns


class UpdateResult:
    def __init__(
            self,
            result: object,
    ):
        self.results: object = result
