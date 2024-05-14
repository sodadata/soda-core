from soda.execution.query.query import Query


class QueryWithoutResults(Query):
    def execute(self):
        for cursor in self._execute_cursor():
            cursor.execute(self.sql)
