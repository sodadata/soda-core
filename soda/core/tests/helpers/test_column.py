from soda.execution.data_type import DataType


class TestColumn:

    def __init__(self, name: str, data_type: DataType):
        self.name: str = name
        self.data_type: DataType = data_type
        # self.actual_name is the actual name in the data source and it is initialized in the TestTableManager.ensure_test_table
        self.actual_name: str = None

    def __hash__(self):
        return

    def to_hashable_json(self):
        """
        Used to create the unique hash part in the table name to determine if the table already exists.
        """
        return [self.name, self.data_type]
