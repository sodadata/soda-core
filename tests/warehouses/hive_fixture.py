from typing import List

from tests.common.warehouse_fixture import WarehouseFixture


class HiveFixture(WarehouseFixture):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def create_database(self):
        pass

    def drop_database(self):
        pass

    def tear_down(self):
        pass
