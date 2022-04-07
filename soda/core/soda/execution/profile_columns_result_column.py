from __future__ import annotations

from typing import Dict, List


class ProfileColumnsResultColumn:

    def __init__(self, column_name: str, column_type: str):
        self.column_name: str = column_name
        self.mins: List[float] | None = None

    def create_column(self, column_name):
        pass

    def get_cloud_dict(self) -> dict:
        cloud_dict = {
            "columnName": self.column_name,
            "profile": {
                "mins": self.mins
            }
        }
        return cloud_dict
